DROP TABLE IF EXISTS transaksi_gate CASCADE;

CREATE TABLE IF NOT EXISTS transaksi_gate (
    id_tiket VARCHAR(50) PRIMARY KEY,
    id_penumpang VARCHAR(50),
    kode_halte VARCHAR(100),
    waktu_tap TIMESTAMP,
    saldo_terpotong INT,
    status_gate VARCHAR(50),
    jam_tap INT,              -- 👈 TAMBAHKAN KOLOM BARU INI
    is_jam_sibuk BOOLEAN       -- 👈 TAMBAHKAN KOLOM BARU INI
);
