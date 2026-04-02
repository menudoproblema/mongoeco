from __future__ import annotations

import math
from typing import Any

from mongoeco.errors import OperationFailure


type GeoPoint = tuple[float, float]
type GeoPolygon = tuple[GeoPoint, ...]


def _require_numeric_coordinate(value: Any, *, label: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
        raise OperationFailure(f"{label} must contain only finite numeric coordinates")
    return float(value)


def parse_geo_point(value: Any, *, label: str = "geo point") -> GeoPoint:
    if isinstance(value, dict):
        if value.get("type") != "Point":
            raise OperationFailure(f"{label} only supports GeoJSON Point values")
        coordinates = value.get("coordinates")
        if not isinstance(coordinates, (list, tuple)) or len(coordinates) != 2:
            raise OperationFailure(f"{label} Point coordinates must be a [x, y] pair")
        return (
            _require_numeric_coordinate(coordinates[0], label=label),
            _require_numeric_coordinate(coordinates[1], label=label),
        )
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return (
            _require_numeric_coordinate(value[0], label=label),
            _require_numeric_coordinate(value[1], label=label),
        )
    raise OperationFailure(f"{label} must be a GeoJSON Point or a legacy [x, y] pair")


def parse_geo_polygon(value: Any, *, label: str = "geo polygon") -> GeoPolygon:
    if not isinstance(value, dict):
        raise OperationFailure(f"{label} must be a GeoJSON geometry document")
    if value.get("type") != "Polygon":
        raise OperationFailure(f"{label} only supports GeoJSON Polygon values")
    coordinates = value.get("coordinates")
    if not isinstance(coordinates, list) or not coordinates:
        raise OperationFailure(f"{label} Polygon coordinates must contain at least one ring")
    outer_ring = coordinates[0]
    if not isinstance(outer_ring, list) or len(outer_ring) < 3:
        raise OperationFailure(f"{label} Polygon outer ring must contain at least three points")
    polygon = tuple(parse_geo_point(point, label=label) for point in outer_ring)
    if polygon[0] != polygon[-1]:
        polygon = polygon + (polygon[0],)
    return polygon


def parse_geo_box(value: Any, *, label: str = "geo box") -> tuple[GeoPoint, GeoPoint]:
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        raise OperationFailure(f"{label} must be a [[x1, y1], [x2, y2]] pair")
    return (
        parse_geo_point(value[0], label=label),
        parse_geo_point(value[1], label=label),
    )


def point_in_box(point: GeoPoint, box: tuple[GeoPoint, GeoPoint]) -> bool:
    (x1, y1), (x2, y2) = box
    min_x, max_x = sorted((x1, x2))
    min_y, max_y = sorted((y1, y2))
    return min_x <= point[0] <= max_x and min_y <= point[1] <= max_y


def point_in_polygon(point: GeoPoint, polygon: GeoPolygon) -> bool:
    if len(polygon) < 4:
        return False
    if any(_point_on_segment(point, polygon[index], polygon[index + 1]) for index in range(len(polygon) - 1)):
        return True
    x, y = point
    inside = False
    for index in range(len(polygon) - 1):
        x1, y1 = polygon[index]
        x2, y2 = polygon[index + 1]
        intersects = (y1 > y) != (y2 > y)
        if not intersects:
            continue
        xinters = x1 + ((y - y1) * (x2 - x1) / (y2 - y1))
        if xinters >= x:
            inside = not inside
    return inside


def planar_distance(left: GeoPoint, right: GeoPoint) -> float:
    return math.hypot(left[0] - right[0], left[1] - right[1])


def point_matches_geometry(point: GeoPoint, geometry_kind: str, geometry: GeoPoint | GeoPolygon | tuple[GeoPoint, GeoPoint]) -> bool:
    if geometry_kind == "point":
        return point == geometry
    if geometry_kind == "polygon":
        return point_in_polygon(point, geometry)
    if geometry_kind == "box":
        return point_in_box(point, geometry)
    raise OperationFailure(f"Unsupported geo geometry kind: {geometry_kind}")


def _point_on_segment(point: GeoPoint, start: GeoPoint, end: GeoPoint) -> bool:
    px, py = point
    x1, y1 = start
    x2, y2 = end
    cross = (px - x1) * (y2 - y1) - (py - y1) * (x2 - x1)
    if abs(cross) > 1e-9:
        return False
    return min(x1, x2) <= px <= max(x1, x2) and min(y1, y2) <= py <= max(y1, y2)
