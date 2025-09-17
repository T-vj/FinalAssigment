from resources.utils.db_conn import conn

def main():
    queries = [
        # 1) Ensure schema exists
        """
        CREATE SCHEMA IF NOT EXISTS bronze;
        """,

        # 2) Create table
        """
        CREATE TABLE bronze.data_pinjaman_nasabah(
            id_transaksi VARCHAR(20),
            id_rekening VARCHAR(20),
            nama VARCHAR(200), 
            alamat VARCHAR(50),
            jumlah_pinjaman VARCHAR(20),
            pendapatan_bulanan VARCHAR(20),
            usia_nasabah VARCHAR(5),
            status_pekerjaan VARCHAR(50),
            Credit_score VARCHAR(5),
            Riwayat_kredit VARCHAR(5),
            Tanggal_data VARCHAR(10)
        );
        """
    ]

    try:
        with conn.cursor() as cur:
            for i, q in enumerate(queries, start=1):
                print(f"Running query {i} ...")
                cur.execute(q)
            conn.commit()
            print("Bronze table created successfully ✅")
    except Exception as e:
        conn.rollback()
        print("Error during execution ❌:", e)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
