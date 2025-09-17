from resources.utils.db_conn import conn

def main():
    queries = [
        """
        WITH cleaned_data AS (
            SELECT
                CAST(TRIM(id_transaksi) AS INT)              AS id_transaksi,
                CAST(TRIM(id_rekening) AS BIGINT)            AS id_rekening,
                CAST(TRIM(nama) AS VARCHAR(50))              AS nama,
                CAST(TRIM(alamat) AS VARCHAR(200))           AS alamat,
                CAST(TRIM(jumlah_pinjaman) AS BIGINT)        AS jumlah_pinjaman,
                CAST(TRIM(pendapatan_bulanan) AS BIGINT)     AS pendapatan_bulanan,
                CAST(TRIM(usia_nasabah) AS INT)              AS usia_nasabah,
                CAST(TRIM(status_pekerjaan) AS VARCHAR(50))  AS status_pekerjaan,
                CAST(TRIM(Riwayat_kredit) AS VARCHAR(5))     AS riwayat_kredit,
                CAST(TRIM(Credit_score) AS INT)              AS credit_score,
                TO_DATE(TRIM(Tanggal_data), 'DD/MM/YYYY')    AS tanggal_data,
                ROW_NUMBER() OVER (
                    PARTITION BY CAST(TRIM(id_transaksi) AS INT)
                    ORDER BY TO_DATE(TRIM(Tanggal_data), 'DD/MM/YYYY') DESC
                ) AS urutan
            FROM bronze.data_pinjaman_nasabah
        )
        INSERT INTO silver.data_pinjaman_nasabah
            (id_transaksi, id_rekening, nama, alamat, jumlah_pinjaman,
             pendapatan_bulanan, usia_nasabah, status_pekerjaan,
             Riwayat_kredit, Credit_score, Tanggal_data)
        SELECT
            id_transaksi, id_rekening, nama, alamat, jumlah_pinjaman,
            pendapatan_bulanan, usia_nasabah, status_pekerjaan,
            riwayat_kredit, credit_score, tanggal_data
        FROM cleaned_data
        WHERE urutan = 1;  -- Ambil baris terbaru per id_transaksi
        """
    ]

    try:
        with conn.cursor() as cur:
            for i, q in enumerate(queries, start=1):
                print(f"Running query {i} ...")
                cur.execute(q)
            conn.commit()
            print("Insert ke silver.data_pinjaman_nasabah sukses ✅")
    except Exception as e:
        conn.rollback()
        print("Error during execution ❌:", e)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
