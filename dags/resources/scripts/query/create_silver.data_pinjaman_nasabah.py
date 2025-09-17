from resources.utils.db_conn import conn

def main():
    queries = [
        # 1) Buat schema silver jika belum ada
        """
        CREATE SCHEMA IF NOT EXISTS silver;
        """,

        # 2) Buat tabel silver sesuai DDL
        """
        CREATE TABLE IF NOT EXISTS silver.data_pinjaman_nasabah (
            id_transaksi INT PRIMARY KEY,
            id_rekening BIGINT,
            nama VARCHAR(50), 
            alamat VARCHAR(200),
            jumlah_pinjaman BIGINT,
            pendapatan_bulanan BIGINT,
            usia_nasabah INT,
            status_pekerjaan VARCHAR(50),
            Riwayat_kredit VARCHAR(5),
            Credit_score INT,
            Tanggal_data DATE
        );
        """,

        # 3) Kosongkan tabel agar insert bersih/idempotent
        """
        TRUNCATE TABLE silver.data_pinjaman_nasabah;
        """,

        # 4) Bersihkan & dedup data dari bronze, lalu insert ke silver
        """
        WITH cleaned_data AS (
            SELECT
                CAST(TRIM(id_transaksi) AS INT)         AS id_transaksi,
                CAST(TRIM(id_rekening) AS BIGINT)       AS id_rekening,
                CAST(TRIM(nama) AS VARCHAR(50))         AS nama,
                CAST(TRIM(alamat) AS VARCHAR(200))      AS alamat,
                CAST(TRIM(jumlah_pinjaman) AS BIGINT)   AS jumlah_pinjaman,
                CAST(TRIM(pendapatan_bulanan) AS BIGINT)AS pendapatan_bulanan,
                CAST(TRIM(usia_nasabah) AS INT)         AS usia_nasabah,
                CAST(TRIM(status_pekerjaan) AS VARCHAR(50)) AS status_pekerjaan,
                CAST(TRIM(Riwayat_kredit) AS VARCHAR(5))    AS Riwayat_kredit,
                CAST(TRIM(Credit_score) AS INT)         AS Credit_score,
                CAST(TRIM(Tanggal_data) AS DATE)        AS Tanggal_data,
                ROW_NUMBER() OVER (
                    PARTITION BY CAST(TRIM(id_transaksi) AS INT)
                    ORDER BY CAST(TRIM(Tanggal_data) AS DATE) DESC
                ) AS urutan
            FROM bronze.data_pinjaman_nasabah
        )
        INSERT INTO silver.data_pinjaman_nasabah (
            id_transaksi, id_rekening, nama, alamat, jumlah_pinjaman,
            pendapatan_bulanan, usia_nasabah, status_pekerjaan,
            Riwayat_kredit, Credit_score, Tanggal_data
        )
        SELECT
            id_transaksi, id_rekening, nama, alamat, jumlah_pinjaman,
            pendapatan_bulanan, usia_nasabah, status_pekerjaan,
            Riwayat_kredit, Credit_score, Tanggal_data
        FROM cleaned_data
        WHERE urutan = 1;
        """
    ]

    try:
        with conn.cursor() as cur:
            for i, q in enumerate(queries, start=1):
                print(f"Running query {i} ...")
                cur.execute(q)
            conn.commit()
            print("Silver table created & populated successfully ✅")
    except Exception as e:
        conn.rollback()
        print("Error during execution ❌:", e)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
