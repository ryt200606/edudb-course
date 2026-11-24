"""Verify extended statistics and the NULL anti join through the real server."""
import argparse
from pathlib import Path
import tempfile

from acceptance import Server
from kernel_benchmark import save_receipt,sha_file,verified

class TestServer(Server):
    def sql(self,sid,statement):
        return verified(self.api("/sessions/"+sid+"/sql","POST",{"sql":statement},timeout=180))

def check(ok,message):
    if not ok:raise AssertionError(message)

def observe(db,sid,table,predicate):
    r=db.sql(sid,f"SELECT id FROM {table} WHERE {predicate}")
    s=r["plan"]["sources"][0]
    return {"predicate":predicate,"estimated_rows":s["estimated_rows"],"actual_rows":len(r["rows"]),
            "index":s["index"],"used":s["joint_statistics_used"]}

def model(db,sid,table,n):
    db.sql(sid,f"CREATE TABLE {table}(id INTEGER PRIMARY KEY,a INTEGER,b INTEGER)")
    for start in range(0,n,50):
        values=[f"({i},{i%10},{i%10 if table=='paired' else i//10%10})" for i in range(start,start+50)]
        db.sql(sid,f"INSERT INTO {table} VALUES "+",".join(values))
    db.sql(sid,f"CREATE INDEX {table}_a ON {table}(a)")
    db.sql(sid,f"CREATE INDEX {table}_b ON {table}(b)")

def anti_join(db,sid):
    db.sql(sid,"CREATE TABLE outer_rows(id INTEGER PRIMARY KEY,k INTEGER)")
    db.sql(sid,"CREATE TABLE inner_rows(id INTEGER PRIMARY KEY,k REAL)")
    db.sql(sid,"INSERT INTO inner_rows VALUES "+",".join(f"({i},{i}.0)" for i in range(32)))
    db.sql(sid,"INSERT INTO outer_rows VALUES "+",".join(f"({i},{'999' if i<2 else 'NULL' if i<4 else '31'})" for i in range(12)))
    statement="SELECT o.id FROM outer_rows o WHERE o.k IS NULL AND NOT EXISTS(SELECT 1 FROM inner_rows i WHERE i.k=o.k) ORDER BY o.id"
    r=db.sql(sid,statement)
    decision=r["plan"]["decorrelation"];actual=r["plan"]["decorrelation_actual"][0]
    check(r["rows"]==[["2"],["3"]] and decision["chosen"] and actual["inner_loops"]==0,"NULL anti join")
    return {"sql":statement,"rows":r["rows"],"decision":decision,"inner_actual":actual}

def lifetime(exe,web,where,mode):
    def serve():return TestServer(exe,where,web,frames=128,extra_args=["--recovery-mode",mode])
    # The main model uses 1000 rows per relation. A second WAL protocol
    # repeats the same visibility and crash invariants on 100 rows per relation.
    n=1000 if mode=="REDO_PRIVATE" else 100
    trace={"mode":mode,"rows_per_table":n,"tables":{},"reopens":[]}
    q="a=2 AND b=2"
    with serve() as db:
        sid=db.session()
        for name,equal,incompatible in (("paired",n//10,0),("mixed",n//100,n//100)):
            model(db,sid,name,n)
            analyze=f"ANALYZE {name} WITH(sample_rows=1200,target=128)"
            db.sql(sid,analyze)
            before=observe(db,sid,name,q)
            db.sql(sid,f"CREATE STATISTICS s_{name} (mcv) ON a,b FROM {name}")
            db.sql(sid,analyze)
            after=observe(db,sid,name,q);opposite=observe(db,sid,name,"a=2 AND b=3")
            check(abs(before["estimated_rows"]-n//100)<1e-6 and not before["used"],"independent estimate")
            for r,expected in ((after,equal),(opposite,incompatible)):
                check(abs(r["estimated_rows"]-expected)<1e-6 and r["actual_rows"]==expected and r["used"]==["s_"+name],"full-scan joint estimate")
            trace["tables"][name]={"before":before,"fresh":after,"incompatible":opposite}
        db.sql(sid,"BEGIN")
        db.sql(sid,"UPDATE paired SET b=3 WHERE id=2")
        stale=observe(db,sid,"paired",q)
        check(db.sql(sid,"SHOW STATISTICS FROM paired")["rows"][0][2]=="stale" and not stale["used"] and stale["actual_rows"]==n//10-1,"stale fallback")
        db.sql(sid,"ROLLBACK")
        restored=observe(db,sid,"paired",q)
        check(restored==trace["tables"]["paired"]["fresh"],"rollback must restore fresh estimate")
        db.sql(sid,"INSERT INTO paired VALUES(1000,NULL,2)")
        null_stale=observe(db,sid,"paired","a IS NULL AND b=2")
        db.sql(sid,"ANALYZE paired WITH(sample_rows=1200,target=128)")
        null_fresh=observe(db,sid,"paired","a IS NULL AND b=2")
        check(not null_stale["used"] and null_fresh["actual_rows"]==1 and
              abs(null_fresh["estimated_rows"]-1)<1e-6 and null_fresh["used"]==["s_paired"],"NULL MCV")
        trace.update(uncommitted_stale=stale,rollback_fresh=restored,committed_stale=null_stale,null_reanalyzed=null_fresh)
        if mode=="REDO_PRIVATE":trace["null_anti_join"]=anti_join(db,sid)
        db.sql(sid,"BEGIN")
        db.sql(sid,"DROP STATISTICS s_paired")
        check(not db.sql(sid,"SHOW STATISTICS FROM paired")["rows"],"uncommitted DROP visible to its owner")
        db.process.kill();db.process.wait(timeout=15)
    for _ in range(2):
        with serve() as db:
            sid=db.session()
            status=db.sql(sid,"SHOW STATISTICS FROM paired")["rows"]
            after=observe(db,sid,"paired","a IS NULL AND b=2")
            check(status[0][2]=="fresh" and after==null_fresh,"committed ANALYZE survives process loss and repeated restart")
            trace["reopens"].append({"status":status,"null_estimate":after})
    return trace

def main():
    cli=argparse.ArgumentParser(description=__doc__)
    for name in ("exe","web","output"):cli.add_argument("--"+name,type=Path,required=True)
    a=cli.parse_args()
    with tempfile.TemporaryDirectory(dir=a.output.parent) as tmp:
        tracing=[lifetime(a.exe.resolve(),a.web.resolve(),Path(tmp)/mode,mode) for mode in ("REDO_PRIVATE","ARIES_PAGE")]
    save_receipt(a.output,{"status":"PASS","binary_sha256":sha_file(a.exe),"observations":tracing})
    print("PASS: correlated and mixed marginals, stale, rollback, NULL, anti join and two post-kill opens in each mode")

if __name__=="__main__":main()
