from resources.utils.db_conn import conn

def main():
    queries = [
        # 1) Buat schema GOLD jika belum ada
        """
        CREATE SCHEMA IF NOT EXISTS GOLD;
        """,

        # 2) Buat tabel GOLD.LAPORAN_PINJAMAN_KREDIT jika belum ada
        """
        CREATE TABLE IF NOT EXISTS GOLD.LAPORAN_PINJAMAN_KREDIT(
            TANGGAL_DATA DATE PRIMARY KEY,
            Total_Pinjaman NUMERIC(18, 2),
            Jumlah_Pinjaman BIGINT,
            rata_rata_Credit_Score NUMERIC(18, 2)
        );
        """
    ]

    try:
        with conn.cursor() as cur:
            for i, q in enumerate(queries, start=1):
                print(f"Running query {i} ...")
                cur.execute(q)
            conn.commit()
            print("Schema & table GOLD dibuat sukses ✅")
    except Exception as e:
        conn.rollback()
        print("Error during execution ❌:", e)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
