from abc import ABC, abstractmethod
from typing import Any


class BenchmarkEngine(ABC):
    @abstractmethod
    def setup(self) -> None:
        """Inicializa el motor y la conexión."""
        pass

    @abstractmethod
    def teardown(self) -> None:
        """Limpia los recursos (cierra conexiones, borra archivos temporales)."""
        pass

    @abstractmethod
    def drop_collection(self, db_name: str, coll_name: str) -> None:
        """Borra una colección."""
        pass

    @abstractmethod
    def insert_many(self, db_name: str, coll_name: str, documents: list[dict[str, Any]]) -> None:
        """Inserta un lote de documentos."""
        pass

    @abstractmethod
    def create_index(
        self,
        db_name: str,
        coll_name: str,
        keys: list[tuple[str, int]],
    ) -> None:
        """Crea un indice simple para el workload."""
        pass

    @abstractmethod
    def find(self, db_name: str, coll_name: str, filter_spec: dict[str, Any], sort: list[tuple[str, int]] | None = None, limit: int = 0) -> list[dict[str, Any]]:
        """Busca documentos y devuelve la lista materializada."""
        pass

    @abstractmethod
    def find_first(
        self,
        db_name: str,
        coll_name: str,
        filter_spec: dict[str, Any],
        sort: list[tuple[str, int]] | None = None,
    ) -> dict[str, Any] | None:
        """Busca documentos y consume solo el primero."""
        pass

    @abstractmethod
    def explain_find(
        self,
        db_name: str,
        coll_name: str,
        filter_spec: dict[str, Any],
        sort: list[tuple[str, int]] | None = None,
        limit: int = 0,
    ) -> dict[str, Any]:
        """Explica un find con el backend actual."""
        pass

    @abstractmethod
    def aggregate(self, db_name: str, coll_name: str, pipeline: list[dict[str, Any]], allow_disk_use: bool = False) -> list[dict[str, Any]]:
        """Ejecuta un pipeline de agregación y devuelve la lista materializada."""
        pass

    @abstractmethod
    def aggregate_first(
        self,
        db_name: str,
        coll_name: str,
        pipeline: list[dict[str, Any]],
        allow_disk_use: bool = False,
    ) -> dict[str, Any] | None:
        """Ejecuta un pipeline y consume solo el primer documento."""
        pass

    @abstractmethod
    def explain_aggregate(
        self,
        db_name: str,
        coll_name: str,
        pipeline: list[dict[str, Any]],
        allow_disk_use: bool = False,
    ) -> dict[str, Any]:
        """Explica un aggregate con el backend actual."""
        pass

    @property
    def label(self) -> str:
        """Etiqueta legible para reportes."""
        return self.__class__.__name__
