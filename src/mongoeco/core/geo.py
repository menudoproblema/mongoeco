from __future__ import annotations

import math
from typing import Any

from shapely.geometry import (
    GeometryCollection,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
    box as shapely_box,
    shape,
)
from shapely.geometry.base import BaseGeometry

from mongoeco.errors import OperationFailure


type GeoPoint = tuple[float, float]
type GeoShape = BaseGeometry

_SUPPORTED_GEOJSON_TYPES = frozenset(
    {
        'Point',
        'LineString',
        'Polygon',
        'MultiPoint',
        'MultiLineString',
        'MultiPolygon',
        'GeometryCollection',
    }
)
_GEOJSON_KIND_NAMES = {
    'Point': 'point',
    'LineString': 'linestring',
    'Polygon': 'polygon',
    'MultiPoint': 'multipoint',
    'MultiLineString': 'multilinestring',
    'MultiPolygon': 'multipolygon',
    'GeometryCollection': 'geometrycollection',
}


def _require_numeric_coordinate(value: Any, *, label: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
        raise OperationFailure(f'{label} must contain only finite numeric coordinates')
    return float(value)


def parse_geo_point(value: Any, *, label: str = 'geo point') -> GeoPoint:
    if isinstance(value, dict):
        if value.get('type') != 'Point':
            raise OperationFailure(f'{label} only supports GeoJSON Point values')
        coordinates = value.get('coordinates')
        if not isinstance(coordinates, (list, tuple)) or len(coordinates) != 2:
            raise OperationFailure(f'{label} Point coordinates must be a [x, y] pair')
        return (
            _require_numeric_coordinate(coordinates[0], label=label),
            _require_numeric_coordinate(coordinates[1], label=label),
        )
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return (
            _require_numeric_coordinate(value[0], label=label),
            _require_numeric_coordinate(value[1], label=label),
        )
    raise OperationFailure(f'{label} must be a GeoJSON Point or a legacy [x, y] pair')


def parse_geo_polygon(value: Any, *, label: str = 'geo polygon') -> GeoShape:
    geometry_kind, geometry = parse_geo_geometry(value, label=label, allow_legacy_point=False)
    if geometry_kind != 'polygon':
        raise OperationFailure(f'{label} only supports GeoJSON Polygon values')
    return geometry


def parse_geo_box(value: Any, *, label: str = 'geo box') -> GeoShape:
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        raise OperationFailure(f'{label} must be a [[x1, y1], [x2, y2]] pair')
    start = parse_geo_point(value[0], label=label)
    end = parse_geo_point(value[1], label=label)
    min_x, max_x = sorted((start[0], end[0]))
    min_y, max_y = sorted((start[1], end[1]))
    return shapely_box(min_x, min_y, max_x, max_y)


def parse_geo_geometry(
    value: Any,
    *,
    label: str = 'geo geometry',
    allow_legacy_point: bool = True,
) -> tuple[str, GeoShape]:
    if allow_legacy_point and isinstance(value, (list, tuple)) and len(value) == 2:
        point = parse_geo_point(value, label=label)
        return 'point', Point(point)
    if not isinstance(value, dict):
        raise OperationFailure(f'{label} must be a supported GeoJSON geometry document')
    geometry_type = value.get('type')
    if not isinstance(geometry_type, str) or geometry_type not in _SUPPORTED_GEOJSON_TYPES:
        raise OperationFailure(
            f'{label} only supports GeoJSON '
            + ', '.join(sorted(_SUPPORTED_GEOJSON_TYPES))
        )
    try:
        geometry = shape(value)
    except Exception as exc:  # pragma: no cover - defensive wrapper over shapely validation
        raise OperationFailure(f'{label} is not a valid GeoJSON geometry: {exc}') from exc
    if geometry.is_empty:
        raise OperationFailure(f'{label} must not be empty')
    _validate_geometry_coordinates(geometry, label=label)
    return _GEOJSON_KIND_NAMES[geometry_type], geometry


def point_in_box(point: GeoPoint, box_geometry: GeoShape) -> bool:
    return box_geometry.covers(Point(point))


def point_in_polygon(point: GeoPoint, polygon: GeoShape) -> bool:
    return polygon.covers(Point(point))


def planar_distance(left: GeoPoint, right: GeoPoint) -> float:
    return math.hypot(left[0] - right[0], left[1] - right[1])


def planar_distance_to_geometry(point: GeoPoint, geometry: GeoShape) -> float:
    return Point(point).distance(geometry)


def point_matches_geometry(point: GeoPoint, geometry_kind: str, geometry: GeoShape) -> bool:
    if geometry_kind == 'point':
        return geometry.equals(Point(point))
    return geometry.covers(Point(point))


def geometry_within_geometry(candidate: GeoShape, container: GeoShape) -> bool:
    return container.covers(candidate)


def geometry_intersects_geometry(left: GeoShape, right: GeoShape) -> bool:
    return left.intersects(right)


def _validate_geometry_coordinates(geometry: GeoShape, *, label: str) -> None:
    for x, y in _iter_geometry_coordinates(geometry):
        _require_numeric_coordinate(x, label=label)
        _require_numeric_coordinate(y, label=label)


def _iter_geometry_coordinates(geometry: GeoShape):
    if isinstance(geometry, Point):
        yield geometry.x, geometry.y
        return
    if isinstance(geometry, LineString):
        for x, y in geometry.coords:
            yield x, y
        return
    if isinstance(geometry, MultiPoint):
        for item in geometry.geoms:
            yield item.x, item.y
        return
    if isinstance(geometry, Polygon):
        yield from geometry.exterior.coords
        for ring in geometry.interiors:
            yield from ring.coords
        return
    if isinstance(geometry, MultiLineString | MultiPolygon | GeometryCollection):
        for item in geometry.geoms:
            yield from _iter_geometry_coordinates(item)
        return
    raise OperationFailure(f'unsupported planar geometry type: {type(geometry)!r}')
