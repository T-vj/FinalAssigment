from resources.utils.db_conn import conn

def main():
    queries = [
        # Pastikan schema gold tersedia
        """
        CREATE SCHEMA IF NOT EXISTS gold;
        """,

        # Tabel Dimensi: Nasabah
        """
        CREATE TABLE IF NOT EXISTS gold.dim_Nasabah (
            ID_Rekening BIGINT PRIMARY KEY,
            nama_lengkap VARCHAR(255),
            alamat VARCHAR(255),
            tanggal_daftar DATE
        );
        """,

        # Tabel Dimensi: Waktu
        """
        CREATE TABLE IF NOT EXISTS gold.dim_Waktu (
            Tanggal_data DATE PRIMARY KEY,
            hari VARCHAR(20),
            bulan INT,
            tahun INT
        );
        """,

        # "Fakta" Transaksi (mengikuti nama tabel yang Anda berikan)
        """
        CREATE TABLE IF NOT EXISTS gold.dim_Transaksi (
            Id_transaksi INT PRIMARY KEY,
            id_rekening BIGINT,
            tanggal_data DATE
        );
        """
    ]

    try:
        with conn.cursor() as cur:
            for i, q in enumerate(queries, start=1):
                print(f"Running query {i} ...")
                cur.execute(q)
            conn.commit()
            print("Tabel dim_Nasabah, dim_Waktu, dan dim_Transaksi berhasil dibuat ✅")
    except Exception as e:
        conn.rollback()
        print("Error during execution ❌:", e)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
