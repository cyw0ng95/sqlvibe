package svdbcgo_test

import (
"testing"
svdbcgo "github.com/cyw0ng95/sqlvibe/internal/cgo"
)

func TestCppEngine_OrderBy(t *testing.T) {
db, err := svdbcgo.Open(":memory:")
if err != nil { t.Fatal(err) }
defer db.Close()
if _, err := db.Exec("CREATE TABLE t(id INT, name TEXT)"); err != nil { t.Fatal(err) }
db.Exec("INSERT INTO t VALUES(3,'c')")
db.Exec("INSERT INTO t VALUES(1,'a')")
db.Exec("INSERT INTO t VALUES(2,'b')")
rows, err := db.Query("SELECT id, name FROM t ORDER BY id ASC")
if err != nil { t.Fatal(err) }
defer rows.Close()
expected := []int64{1, 2, 3}
for i, exp := range expected {
if !rows.Next() { t.Fatalf("expected row %d", i) }
v := rows.Get(0).(int64)
if v != exp { t.Errorf("row %d: got %d want %d", i, v, exp) }
}
}

func TestCppEngine_Limit(t *testing.T) {
db, _ := svdbcgo.Open(":memory:")
defer db.Close()
db.Exec("CREATE TABLE t(id INT)")
for i := 1; i <= 5; i++ {
db.Exec("INSERT INTO t VALUES(?)" )  // won't work with ?; use literal
}
db.Exec("INSERT INTO t VALUES(1)")
db.Exec("INSERT INTO t VALUES(2)")
db.Exec("INSERT INTO t VALUES(3)")
db.Exec("INSERT INTO t VALUES(4)")
db.Exec("INSERT INTO t VALUES(5)")
rows, _ := db.Query("SELECT id FROM t ORDER BY id LIMIT 3")
defer rows.Close()
count := 0
for rows.Next() { count++ }
if count != 3 { t.Errorf("expected 3 rows, got %d", count) }
}

func TestCppEngine_GroupByCount(t *testing.T) {
db, _ := svdbcgo.Open(":memory:")
defer db.Close()
db.Exec("CREATE TABLE t(cat TEXT, val INT)")
db.Exec("INSERT INTO t VALUES('A', 10)")
db.Exec("INSERT INTO t VALUES('A', 20)")
db.Exec("INSERT INTO t VALUES('B', 30)")
rows, err := db.Query("SELECT cat, COUNT(*) FROM t GROUP BY cat ORDER BY cat")
if err != nil { t.Fatal(err) }
defer rows.Close()
if !rows.Next() { t.Fatal("no rows") }
cat := rows.Get(0).(string)
cnt := rows.Get(1).(int64)
if cat != "A" || cnt != 2 { t.Errorf("got cat=%s cnt=%d", cat, cnt) }
if !rows.Next() { t.Fatal("expected second row") }
cat = rows.Get(0).(string)
cnt = rows.Get(1).(int64)
if cat != "B" || cnt != 1 { t.Errorf("got cat=%s cnt=%d", cat, cnt) }
}

func TestCppEngine_InnerJoin(t *testing.T) {
db, _ := svdbcgo.Open(":memory:")
defer db.Close()
db.Exec("CREATE TABLE users(id INT, name TEXT)")
db.Exec("CREATE TABLE orders(user_id INT, amount INT)")
db.Exec("INSERT INTO users VALUES(1,'Alice')")
db.Exec("INSERT INTO users VALUES(2,'Bob')")
db.Exec("INSERT INTO orders VALUES(1, 100)")
db.Exec("INSERT INTO orders VALUES(1, 200)")
rows, err := db.Query("SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id ORDER BY orders.amount")
if err != nil { t.Fatal(err) }
defer rows.Close()
count := 0
for rows.Next() { count++ }
if count != 2 { t.Errorf("expected 2 rows, got %d", count) }
}
