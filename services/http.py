import requests
from config.logger import setup_logger

logger = setup_logger(__name__)

class Http:
    @staticmethod
    def http_get(url: str, data: dict = None, timeout: int = 10, headers: dict = None):
        try:
            default_headers = {
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0'
            }

            if headers:
                default_headers.update(headers)

            response = requests.get(url, json=data, headers=default_headers, timeout=timeout)

            logger.info(f"Response Status: {response.status_code}")

            # otomatis parse JSON kalau bisa
            try:
                return response.json()
            except ValueError:
                return response.text

        except requests.exceptions.Timeout:
            logger.error("Request timed out")
            return None

        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None
