# Bungkus parameter query dengan function dibawah
def params(value):
    """Pastikan nilai adalah tuple. Jika bukan, bungkus menjadi tuple."""
    if isinstance(value, (tuple, list)):
        return tuple(value)  # Pastikan dalam bentuk tuple
    return (value,)  # Bungkus menjadi tuple jika hanya satu nilai