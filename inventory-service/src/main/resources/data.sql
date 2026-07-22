INSERT INTO products (id, name, price, stock) VALUES ('P-1001', 'Clavier mécanique', 89.90, 50) ON CONFLICT (id) DO NOTHING;
INSERT INTO products (id, name, price, stock) VALUES ('P-1002', 'Souris sans fil', 39.90, 100) ON CONFLICT (id) DO NOTHING;
INSERT INTO products (id, name, price, stock) VALUES ('P-1003', 'Écran 27 pouces', 249.00, 25) ON CONFLICT (id) DO NOTHING;
INSERT INTO products (id, name, price, stock) VALUES ('P-1004', 'Casque audio', 129.00, 40) ON CONFLICT (id) DO NOTHING;
INSERT INTO products (id, name, price, stock) VALUES ('P-1005', 'Webcam HD', 59.90, 10) ON CONFLICT (id) DO NOTHING;
INSERT INTO products (id, name, price, stock) VALUES ('P-1006', 'Dock USB-C', 79.90, 3) ON CONFLICT (id) DO NOTHING;
