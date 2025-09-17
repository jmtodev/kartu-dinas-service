from abc import ABC, abstractmethod

# Kelas abstrak untuk konektor database
class BaseDatabaseConnector(ABC):
    @abstractmethod
    def connect(self):
        """Membuka koneksi ke database"""
        pass

    @abstractmethod
    def fetch(self, query: str, params: tuple = ()):
        """Menjalankan query SELECT dan mengembalikan hasil"""
        pass

    @abstractmethod
    def execute(self, query: str, params: tuple = ()):
        """Menjalankan query non-SELECT (INSERT, UPDATE, DELETE)"""
        pass

    @abstractmethod
    def commit(self):
        """Menyimpan perubahan ke database"""
        pass

    @abstractmethod
    def close(self):
        """Menutup koneksi dan resource terkait"""
        pass
