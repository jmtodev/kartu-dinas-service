# WKD Service

Service Python untuk mengambil data distribusi dari API WKD dan menyimpan/memperbarui data ke database MySQL.

Service yang tersedia:

- `whitelist`
- `blacklist`
- `penerbitan`

---

# Requirements

Install dependency dari file `requirements.txt`:

```bash
pip install -r requirements.txt
```

---

# Environment Variables

| Variable            | Description                                                               |
| ------------------- | ------------------------------------------------------------------------- |
| `SERVICE`           | Nama service yang dijalankan: `whitelist`, `blacklist`, atau `penerbitan` |
| `ENDPOINT_URL`      | Base URL backend API                                                      |
| `SCHEDULE_INTERVAL` | Interval scheduler dalam menit                                            |
| `USERNAME`          | Username client untuk request token                                       |
| `PASSWORD`          | Password client untuk request token                                       |
| `DB_HOST`           | Host database MySQL                                                       |
| `DB_USERNAME`       | Username database                                                         |
| `DB_PORT`           | Port database MySQL                                                       |
| `DB_PASSWORD`       | Password database                                                         |
| `DB_DATABASE`       | Nama database                                                             |

---

# Menjalankan di Local

Contoh menjalankan service whitelist:

```bash
SERVICE="whitelist" ENDPOINT_URL="http://localhost:3000" SCHEDULE_INTERVAL=1 USERNAME="haidar" PASSWORD="Password123!" DB_HOST="10.10.200.20" DB_USERNAME="iot" DB_PORT="3306" DB_PASSWORD="password" DB_DATABASE="jago_bcds" python main.py
```

Untuk service lain, ganti value `SERVICE`:

```bash
SERVICE="blacklist" ...
```

atau:

```bash
SERVICE="penerbitan" ...
```

---

# Menjalankan dengan Docker

Contoh menjalankan service whitelist:

```bash
docker run --restart=always \
  -v /etc/localtime:/etc/localtime \
  --name WhitelistService \
  -e SERVICE="whitelist" \
  -e ENDPOINT_URL="http://10.10.100.113:3000" \
  -e SCHEDULE_INTERVAL=1 \
  -e USERNAME="" \
  -e PASSWORD="" \
  -e DB_HOST="10.10.200.20" \
  -e DB_USERNAME="iot" \
  -e DB_PORT="3306" \
  -e DB_PASSWORD="password" \
  -e DB_DATABASE="jago_bcds" \
  -dit haidarijlal/wkd-service:latest
```

---

# Build Docker Image

Build image:

```bash
docker build -t namahub/images .
```

Contoh:

```bash
docker build -t haidarijlal/wkd-service:latest .
```

---

# Push ke Docker Hub

Login Docker Hub terlebih dahulu:

```bash
docker login
```

Push image:

```bash
docker push namahub/images
```

Contoh:

```bash
docker push haidarijlal/wkd-service:latest
```

---

# Deskripsi Fungsi

## `run_service()`

Fungsi utama untuk menjalankan service.

Alur kerja:

1. Membuka koneksi database.
2. Mengambil data dari API.
3. Memvalidasi response API.
4. Melakukan mapping data.
5. Menyimpan atau memperbarui data ke database.
6. Menutup koneksi database.

---

## `_request_api_token()`

Digunakan untuk mengambil token baru dari endpoint:

```text
/api/v1/auth/client/requestToken
```

Body request:

```json
{
  "username": "USERNAME",
  "password": "PASSWORD"
}
```

Token hanya diminta ketika:

- token belum ada
- token expired
- response API mengembalikan `401`
- response API mengandung pesan `unauthorized`

---

## `_fetch_from_api()`

Mengatur proses pengambilan data dari API.

Alur kerja:

1. Cek apakah token sudah ada.
2. Jika token belum ada, request token baru.
3. Fetch data dari endpoint service.
4. Jika response unauthorized, request token baru.
5. Retry fetch data sekali lagi.

---

## `_fetch_whitelist()`

Mengambil data whitelist dari endpoint:

```text
/api/v1/distribution/data/whitelist
```

---

## `_fetch_penerbitan()`

Mengambil data penerbitan dari endpoint:

```text
/api/v1/distribution/data/penerbitan
```

---

## `_fetch_blacklist()`

Mengambil data blacklist dari endpoint:

```text
/api/v1/distribution/data/blacklist
```

---

## `_is_unauthorized(response)`

Mengecek apakah response API menandakan token tidak valid.

Kondisi yang dianggap unauthorized:

- `status_code == 401`
- message mengandung `unauthorized`
- message mengandung `expired`
- message mengandung `kadaluarsa`
- message mengandung `kadaluwarsa`

---

## `_map_data(item)`

Mengubah struktur data dari API agar sesuai dengan struktur tabel database.

Setiap service memiliki mapping berbeda:

- `Whitelist` → `tbl_penerbitan_kartu_whitelist`
- `Penerbitan` → `tbl_penerbitan_kartu`
- `Blacklist` → `tbl_blacklist`

---

## `_save_to_db(mapped_data)`

Menyimpan data ke database menggunakan mekanisme upsert:

```sql
INSERT ... ON DUPLICATE KEY UPDATE
```

Jika data dengan unique key yang sama sudah ada, maka data akan di-update.

---

## `_hex_to_decimal_little_endian(hex_str)`

Khusus service `Blacklist`.

Digunakan untuk mengubah UID hexadecimal menjadi decimal dengan format little-endian sebelum disimpan ke kolom `uuid`.

---

# Catatan

Token API tidak di-request setiap kali scheduler berjalan selama proses Python masih hidup.

Token baru hanya akan di-request jika:

- token masih kosong
- request data gagal karena `401`
- response API mengandung pesan unauthorized atau expired
