from resources.utils.db_conn import conn

def main():
    queries = [
        # Insert contoh data "kotor/duplikat" ke bronze
        """
        INSERT INTO bronze.data_pinjaman_nasabah
            (id_transaksi, id_rekening, jumlah_pinjaman, pendapatan_bulanan, usia_nasabah, status_pekerjaan, Credit_Score, riwayat_kredit, tanggal_data)
        VALUES
            ('1','996357569','10000000','11000000','52','wirausaha','508','baik','2025-03-29'),
            ('2','910996621','9000000','11000000','40','wirausaha','610','baik','2024-10-14'),
            ('3','993536939','5000000','6000000','46','pns','691','buruk','2025-01-17'),
            ('4','971171300','6000000','5000000','42','wirausaha','701','baik','2025-07-29'),
            ('5','904435409','10000000','11000000','44','wirausaha','687','baik','2025-01-31'),
            ('6','930573645','12000000','13000000','42','wirausaha','764','baik','2025-09-01'),
            ('7','928563337','10000000','12000000','54','wirausaha','615','baik','2025-01-15'),
            ('8','933407535','14000000','17000000','37','wirausaha','812','baik','2025-06-11');
        """
    ]

    try:
        with conn.cursor() as cur:
            for i, q in enumerate(queries, start=1):
                print(f"Running query {i} ...")
                cur.execute(q)
            conn.commit()
            print("Data contoh berhasil dimasukkan ke bronze ✅")
    except Exception as e:
        conn.rollback()
        print("Error during execution ❌:", e)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
