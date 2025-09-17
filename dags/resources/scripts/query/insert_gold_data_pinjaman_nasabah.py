from resources.utils.db_conn import conn

def main():
    queries = [
        # Agregasi per tanggal ke GOLD.LAPORAN_PINJAMAN_KREDIT
        # Menggunakan upsert agar aman jika baris untuk tanggal yang sama sudah ada.
        """
        INSERT INTO GOLD.LAPORAN_PINJAMAN_KREDIT
            (TANGGAL_DATA, Total_Pinjaman, Jumlah_Pinjaman, rata_rata_Credit_Score)
        SELECT
            tanggal_data::DATE                                        AS TANGGAL_DATA,
            SUM(jumlah_pinjaman)::NUMERIC(18, 2)                      AS Total_Pinjaman,
            COUNT(*)::BIGINT                                          AS Jumlah_Pinjaman,
            AVG(Credit_score)::NUMERIC(18, 2)                         AS rata_rata_Credit_Score
        FROM silver.data_pinjaman_nasabah
        GROUP BY tanggal_data::DATE
        ON CONFLICT (TANGGAL_DATA) DO UPDATE SET
            Total_Pinjaman = EXCLUDED.Total_Pinjaman,
            Jumlah_Pinjaman = EXCLUDED.Jumlah_Pinjaman,
            rata_rata_Credit_Score = EXCLUDED.rata_rata_Credit_Score;
        """
    ]

    try:
        with conn.cursor() as cur:
            for i, q in enumerate(queries, start=1):
                print(f"Running query {i} ...")
                cur.execute(q)
            conn.commit()
            print("LAPORAN_PINJAMAN_KREDIT terisi/terbarui sukses ✅")
    except Exception as e:
        conn.rollback()
        print("Error during execution ❌:", e)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
