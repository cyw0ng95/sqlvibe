package SQLValidator

// tpccSchema holds the column metadata for all TPC-C tables used by the generator.

// colMeta describes a single column in a TPC-C table.
type colMeta struct {
	name    string
	colType string // "INTEGER", "REAL", "TEXT"
	notNull bool
	isPK    bool
}

// tableMeta describes a TPC-C table.
type tableMeta struct {
	name    string
	columns []colMeta
	pkCols  []string // primary-key column names (in order)
}

// tpccTables is the canonical TPC-C schema used by the generator.
var tpccTables = []tableMeta{
	{
		name: "warehouse",
		columns: []colMeta{
			{"w_id", "INTEGER", true, true},
			{"w_name", "TEXT", true, false},
			{"w_street_1", "TEXT", false, false},
			{"w_street_2", "TEXT", false, false},
			{"w_city", "TEXT", false, false},
			{"w_state", "TEXT", false, false},
			{"w_zip", "TEXT", false, false},
			{"w_tax", "REAL", true, false},
			{"w_ytd", "REAL", true, false},
		},
		pkCols: []string{"w_id"},
	},
	{
		name: "district",
		columns: []colMeta{
			{"d_id", "INTEGER", true, true},
			{"d_w_id", "INTEGER", true, true},
			{"d_name", "TEXT", true, false},
			{"d_street_1", "TEXT", false, false},
			{"d_street_2", "TEXT", false, false},
			{"d_city", "TEXT", false, false},
			{"d_state", "TEXT", false, false},
			{"d_zip", "TEXT", false, false},
			{"d_tax", "REAL", true, false},
			{"d_ytd", "REAL", true, false},
			{"d_next_o_id", "INTEGER", true, false},
		},
		pkCols: []string{"d_id", "d_w_id"},
	},
	{
		name: "customer",
		columns: []colMeta{
			{"c_id", "INTEGER", true, true},
			{"c_d_id", "INTEGER", true, true},
			{"c_w_id", "INTEGER", true, true},
			{"c_first", "TEXT", false, false},
			{"c_middle", "TEXT", false, false},
			{"c_last", "TEXT", false, false},
			{"c_street_1", "TEXT", false, false},
			{"c_street_2", "TEXT", false, false},
			{"c_city", "TEXT", false, false},
			{"c_state", "TEXT", false, false},
			{"c_zip", "TEXT", false, false},
			{"c_phone", "TEXT", false, false},
			{"c_credit", "TEXT", false, false},
			{"c_credit_lim", "REAL", false, false},
			{"c_discount", "REAL", false, false},
			{"c_balance", "REAL", true, false},
			{"c_ytd_payment", "REAL", false, false},
			{"c_payment_cnt", "INTEGER", false, false},
			{"c_delivery_cnt", "INTEGER", false, false},
			{"c_data", "TEXT", false, false},
		},
		pkCols: []string{"c_id", "c_d_id", "c_w_id"},
	},
	{
		name: "orders",
		columns: []colMeta{
			{"o_id", "INTEGER", true, true},
			{"o_d_id", "INTEGER", true, true},
			{"o_w_id", "INTEGER", true, true},
			{"o_c_id", "INTEGER", true, false},
			{"o_entry_d", "TEXT", false, false},
			{"o_carrier_id", "INTEGER", false, false},
			{"o_ol_cnt", "INTEGER", true, false},
			{"o_all_local", "INTEGER", true, false},
		},
		pkCols: []string{"o_id", "o_d_id", "o_w_id"},
	},
	{
		name: "order_line",
		columns: []colMeta{
			{"ol_o_id", "INTEGER", true, true},
			{"ol_d_id", "INTEGER", true, true},
			{"ol_w_id", "INTEGER", true, true},
			{"ol_number", "INTEGER", true, true},
			{"ol_i_id", "INTEGER", true, false},
			{"ol_supply_w_id", "INTEGER", true, false},
			{"ol_delivery_d", "TEXT", false, false},
			{"ol_quantity", "INTEGER", true, false},
			{"ol_amount", "REAL", true, false},
			{"ol_dist_info", "TEXT", false, false},
		},
		pkCols: []string{"ol_o_id", "ol_d_id", "ol_w_id", "ol_number"},
	},
	{
		name: "item",
		columns: []colMeta{
			{"i_id", "INTEGER", true, true},
			{"i_im_id", "INTEGER", false, false},
			{"i_name", "TEXT", true, false},
			{"i_price", "REAL", true, false},
			{"i_data", "TEXT", false, false},
		},
		pkCols: []string{"i_id"},
	},
	{
		name: "stock",
		columns: []colMeta{
			{"s_i_id", "INTEGER", true, true},
			{"s_w_id", "INTEGER", true, true},
			{"s_quantity", "INTEGER", true, false},
			{"s_dist_01", "TEXT", false, false},
			{"s_dist_02", "TEXT", false, false},
			{"s_ytd", "REAL", false, false},
			{"s_order_cnt", "INTEGER", false, false},
			{"s_remote_cnt", "INTEGER", false, false},
			{"s_data", "TEXT", false, false},
		},
		pkCols: []string{"s_i_id", "s_w_id"},
	},
}

// tpccDDL returns the CREATE TABLE SQL statements for all TPC-C tables.
const tpccDDL = `
CREATE TABLE warehouse (
    w_id       INTEGER PRIMARY KEY,
    w_name     TEXT    NOT NULL,
    w_street_1 TEXT,
    w_street_2 TEXT,
    w_city     TEXT,
    w_state    TEXT,
    w_zip      TEXT,
    w_tax      REAL    NOT NULL,
    w_ytd      REAL    NOT NULL
);
CREATE TABLE district (
    d_id        INTEGER NOT NULL,
    d_w_id      INTEGER NOT NULL,
    d_name      TEXT    NOT NULL,
    d_street_1  TEXT,
    d_street_2  TEXT,
    d_city      TEXT,
    d_state     TEXT,
    d_zip       TEXT,
    d_tax       REAL    NOT NULL,
    d_ytd       REAL    NOT NULL,
    d_next_o_id INTEGER NOT NULL,
    PRIMARY KEY (d_id, d_w_id)
);
CREATE TABLE customer (
    c_id        INTEGER NOT NULL,
    c_d_id      INTEGER NOT NULL,
    c_w_id      INTEGER NOT NULL,
    c_first     TEXT,
    c_middle    TEXT,
    c_last      TEXT,
    c_street_1  TEXT,
    c_street_2  TEXT,
    c_city      TEXT,
    c_state     TEXT,
    c_zip       TEXT,
    c_phone     TEXT,
    c_credit    TEXT,
    c_credit_lim REAL,
    c_discount  REAL,
    c_balance   REAL    NOT NULL,
    c_ytd_payment REAL,
    c_payment_cnt INTEGER,
    c_delivery_cnt INTEGER,
    c_data      TEXT,
    PRIMARY KEY (c_id, c_d_id, c_w_id)
);
CREATE TABLE orders (
    o_id         INTEGER NOT NULL,
    o_d_id       INTEGER NOT NULL,
    o_w_id       INTEGER NOT NULL,
    o_c_id       INTEGER NOT NULL,
    o_entry_d    TEXT,
    o_carrier_id INTEGER,
    o_ol_cnt     INTEGER NOT NULL,
    o_all_local  INTEGER NOT NULL,
    PRIMARY KEY (o_id, o_d_id, o_w_id)
);
CREATE TABLE order_line (
    ol_o_id        INTEGER NOT NULL,
    ol_d_id        INTEGER NOT NULL,
    ol_w_id        INTEGER NOT NULL,
    ol_number      INTEGER NOT NULL,
    ol_i_id        INTEGER NOT NULL,
    ol_supply_w_id INTEGER NOT NULL,
    ol_delivery_d  TEXT,
    ol_quantity    INTEGER NOT NULL,
    ol_amount      REAL    NOT NULL,
    ol_dist_info   TEXT,
    PRIMARY KEY (ol_o_id, ol_d_id, ol_w_id, ol_number)
);
CREATE TABLE item (
    i_id    INTEGER PRIMARY KEY,
    i_im_id INTEGER,
    i_name  TEXT    NOT NULL,
    i_price REAL    NOT NULL,
    i_data  TEXT
);
CREATE TABLE stock (
    s_i_id       INTEGER NOT NULL,
    s_w_id       INTEGER NOT NULL,
    s_quantity   INTEGER NOT NULL,
    s_dist_01    TEXT,
    s_dist_02    TEXT,
    s_ytd        REAL,
    s_order_cnt  INTEGER,
    s_remote_cnt INTEGER,
    s_data       TEXT,
    PRIMARY KEY (s_i_id, s_w_id)
);
`

// tpccSeedSQL inserts a small deterministic dataset (4 warehouses, 8 districts,
// 20 customers, 10 orders, 30 order lines, 10 items, 20 stock rows).
const tpccSeedSQL = `
INSERT INTO warehouse VALUES (1,'W-Alpha','100 Main St',NULL,'Springfield','IL','62701',0.05,100000.0);
INSERT INTO warehouse VALUES (2,'W-Beta','200 Oak Ave','Suite 5','Shelbyville','IL','62702',0.08,200000.0);
INSERT INTO warehouse VALUES (3,'W-Gamma','300 Pine Rd',NULL,'Capital City','IL','62703',0.10,150000.0);
INSERT INTO warehouse VALUES (4,'W-Delta','400 Elm St',NULL,'Ogdenville','IL','62704',0.06,120000.0);

INSERT INTO district VALUES (1,1,'D-1-1','10 A St',NULL,'Springfield','IL','62701',0.05,50000.0,11);
INSERT INTO district VALUES (2,1,'D-2-1','20 B St',NULL,'Springfield','IL','62701',0.06,60000.0,21);
INSERT INTO district VALUES (1,2,'D-1-2','30 C St',NULL,'Shelbyville','IL','62702',0.07,70000.0,31);
INSERT INTO district VALUES (2,2,'D-2-2','40 D St',NULL,'Shelbyville','IL','62702',0.08,80000.0,41);
INSERT INTO district VALUES (1,3,'D-1-3','50 E St',NULL,'Capital City','IL','62703',0.09,90000.0,51);
INSERT INTO district VALUES (2,3,'D-2-3','60 F St',NULL,'Capital City','IL','62703',0.10,100000.0,61);
INSERT INTO district VALUES (1,4,'D-1-4','70 G St',NULL,'Ogdenville','IL','62704',0.04,40000.0,71);
INSERT INTO district VALUES (2,4,'D-2-4','80 H St',NULL,'Ogdenville','IL','62704',0.03,30000.0,81);

INSERT INTO customer VALUES (1,1,1,'Alice',NULL,'Smith','1 A','2 B','Springfield','IL','62701','555-0001','GC',50000.0,0.10,100.0,50.0,1,0,NULL);
INSERT INTO customer VALUES (2,1,1,'Bob',NULL,'Jones','3 C',NULL,'Springfield','IL','62701','555-0002','BC',50000.0,0.20,200.0,100.0,2,0,NULL);
INSERT INTO customer VALUES (3,1,1,'Carol',NULL,'Brown','5 D',NULL,'Springfield','IL','62701','555-0003','GC',50000.0,0.05,-50.0,0.0,0,0,NULL);
INSERT INTO customer VALUES (1,2,1,'Dave',NULL,'Wilson','7 E',NULL,'Springfield','IL','62701','555-0004','GC',50000.0,0.15,300.0,150.0,3,1,NULL);
INSERT INTO customer VALUES (2,2,1,'Eve',NULL,'Taylor','9 F',NULL,'Springfield','IL','62701','555-0005','BC',50000.0,0.25,400.0,200.0,4,1,NULL);
INSERT INTO customer VALUES (1,1,2,'Frank',NULL,'Anderson','11 G',NULL,'Shelbyville','IL','62702','555-0006','GC',50000.0,0.12,150.0,75.0,1,0,NULL);
INSERT INTO customer VALUES (2,1,2,'Grace',NULL,'Martin','13 H',NULL,'Shelbyville','IL','62702','555-0007','BC',50000.0,0.18,250.0,125.0,2,0,NULL);
INSERT INTO customer VALUES (1,2,2,'Heidi',NULL,'Garcia','15 I',NULL,'Shelbyville','IL','62702','555-0008','GC',50000.0,0.08,350.0,175.0,3,1,NULL);
INSERT INTO customer VALUES (2,2,2,'Ivan',NULL,'Martinez','17 J',NULL,'Shelbyville','IL','62702','555-0009','BC',50000.0,0.22,450.0,225.0,4,1,NULL);
INSERT INTO customer VALUES (1,1,3,'Judy',NULL,'Robinson','19 K',NULL,'Capital City','IL','62703','555-0010','GC',50000.0,0.09,125.0,62.5,1,0,NULL);

INSERT INTO item VALUES (1,101,'Widget-A',9.99,'sample');
INSERT INTO item VALUES (2,102,'Widget-B',14.99,'sample');
INSERT INTO item VALUES (3,103,'Gadget-C',24.99,'original');
INSERT INTO item VALUES (4,104,'Gadget-D',34.99,'original');
INSERT INTO item VALUES (5,105,'Doohickey-E',4.99,'sample');
INSERT INTO item VALUES (6,106,'Doohickey-F',7.49,'sample');
INSERT INTO item VALUES (7,107,'Thingamajig-G',19.99,'original');
INSERT INTO item VALUES (8,108,'Thingamajig-H',29.99,'original');
INSERT INTO item VALUES (9,109,'Whatsit-I',1.99,'sample');
INSERT INTO item VALUES (10,110,'Whatsit-J',2.99,'sample');

INSERT INTO orders VALUES (1,1,1,1,'2024-01-01',1,3,1);
INSERT INTO orders VALUES (2,1,1,2,'2024-01-02',2,2,1);
INSERT INTO orders VALUES (3,1,1,3,'2024-01-03',NULL,1,1);
INSERT INTO orders VALUES (4,2,1,1,'2024-01-04',1,2,1);
INSERT INTO orders VALUES (5,2,1,2,'2024-01-05',NULL,3,1);
INSERT INTO orders VALUES (6,1,2,1,'2024-01-06',2,1,1);
INSERT INTO orders VALUES (7,1,2,2,'2024-01-07',1,2,1);
INSERT INTO orders VALUES (8,2,2,1,'2024-01-08',NULL,2,1);
INSERT INTO orders VALUES (9,1,3,1,'2024-01-09',1,1,1);
INSERT INTO orders VALUES (10,2,4,1,'2024-01-10',2,1,1);

INSERT INTO order_line VALUES (1,1,1,1,1,1,'2024-01-02',1,9.99,'dist-info-1');
INSERT INTO order_line VALUES (1,1,1,2,2,1,'2024-01-02',2,29.98,'dist-info-2');
INSERT INTO order_line VALUES (1,1,1,3,3,1,'2024-01-02',1,24.99,'dist-info-3');
INSERT INTO order_line VALUES (2,1,1,1,4,1,'2024-01-03',1,34.99,'dist-info-4');
INSERT INTO order_line VALUES (2,1,1,2,5,2,'2024-01-03',3,14.97,'dist-info-5');
INSERT INTO order_line VALUES (3,1,1,1,6,1,NULL,1,7.49,'dist-info-6');
INSERT INTO order_line VALUES (4,2,1,1,7,1,'2024-01-05',2,39.98,'dist-info-7');
INSERT INTO order_line VALUES (4,2,1,2,8,2,'2024-01-05',1,29.99,'dist-info-8');
INSERT INTO order_line VALUES (5,2,1,1,9,1,NULL,5,9.95,'dist-info-9');
INSERT INTO order_line VALUES (5,2,1,2,10,3,NULL,2,5.98,'dist-info-10');
INSERT INTO order_line VALUES (5,2,1,3,1,1,NULL,1,9.99,'dist-info-11');
INSERT INTO order_line VALUES (6,1,2,1,2,1,'2024-01-07',1,14.99,'dist-info-12');
INSERT INTO order_line VALUES (7,1,2,1,3,2,'2024-01-08',2,49.98,'dist-info-13');
INSERT INTO order_line VALUES (7,1,2,2,4,1,'2024-01-08',1,34.99,'dist-info-14');
INSERT INTO order_line VALUES (8,2,2,1,5,1,NULL,4,19.96,'dist-info-15');
INSERT INTO order_line VALUES (8,2,2,2,6,2,NULL,1,7.49,'dist-info-16');
INSERT INTO order_line VALUES (9,1,3,1,7,1,'2024-01-10',1,19.99,'dist-info-17');
INSERT INTO order_line VALUES (10,2,4,1,8,1,'2024-01-11',2,59.98,'dist-info-18');

INSERT INTO stock VALUES (1,1,100,'D01-a','D02-a',0.0,0,0,NULL);
INSERT INTO stock VALUES (2,1,200,'D01-b','D02-b',0.0,0,0,NULL);
INSERT INTO stock VALUES (3,1,150,'D01-c','D02-c',0.0,0,0,'original');
INSERT INTO stock VALUES (4,1,175,'D01-d','D02-d',0.0,0,0,'original');
INSERT INTO stock VALUES (5,2,120,'D01-e','D02-e',0.0,0,0,NULL);
INSERT INTO stock VALUES (6,2,130,'D01-f','D02-f',0.0,0,0,NULL);
INSERT INTO stock VALUES (7,2,140,'D01-g','D02-g',0.0,0,0,'original');
INSERT INTO stock VALUES (8,2,160,'D01-h','D02-h',0.0,0,0,'original');
INSERT INTO stock VALUES (1,3,90,'D01-i','D02-i',0.0,0,0,NULL);
INSERT INTO stock VALUES (2,3,80,'D01-j','D02-j',0.0,0,0,NULL);
INSERT INTO stock VALUES (3,4,70,'D01-k','D02-k',0.0,0,0,NULL);
INSERT INTO stock VALUES (4,4,60,'D01-l','D02-l',0.0,0,0,NULL);
INSERT INTO stock VALUES (5,1,110,'D01-m','D02-m',500.0,5,1,NULL);
INSERT INTO stock VALUES (6,1,125,'D01-n','D02-n',250.0,2,0,NULL);
INSERT INTO stock VALUES (7,3,135,'D01-o','D02-o',0.0,0,0,NULL);
INSERT INTO stock VALUES (8,3,145,'D01-p','D02-p',0.0,0,0,'original');
INSERT INTO stock VALUES (9,1,155,'D01-q','D02-q',0.0,0,0,'sample');
INSERT INTO stock VALUES (10,1,165,'D01-r','D02-r',0.0,0,0,'sample');
INSERT INTO stock VALUES (9,2,175,'D01-s','D02-s',0.0,0,0,NULL);
INSERT INTO stock VALUES (10,2,185,'D01-t','D02-t',0.0,0,0,NULL);
`

// tableByName returns the tableMeta for a given name, or nil if not found.
func tableByName(name string) *tableMeta {
	for i := range tpccTables {
		if tpccTables[i].name == name {
			return &tpccTables[i]
		}
	}
	return nil
}

// nonNullIntCols returns the names of all NOT NULL INTEGER columns in a table.
func (tm *tableMeta) nonNullIntCols() []string {
	var out []string
	for _, c := range tm.columns {
		if c.colType == "INTEGER" && c.notNull {
			out = append(out, c.name)
		}
	}
	return out
}

// allColNames returns all column names in order.
func (tm *tableMeta) allColNames() []string {
	names := make([]string, len(tm.columns))
	for i, c := range tm.columns {
		names[i] = c.name
	}
	return names
}
