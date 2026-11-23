set pagination off
set confirm off
set breakpoint pending off
set print pretty off
set print elements 16
directory /opt/db/pgcourse/build/postgresql-14.11
set $hits1=0
break btgettuple if scan->indexRelation->rd_id == 16389
commands
silent
set $hits1=$hits1+1
printf "\n=== btgettuple hit=%d ===\n", $hits1
print scan->indexRelation->rd_id
print dir
print ((BTScanOpaqueData*)scan->opaque)->currPos.currPage
print ((BTScanOpaqueData*)scan->opaque)->currPos.firstItem
print ((BTScanOpaqueData*)scan->opaque)->currPos.lastItem
print ((BTScanOpaqueData*)scan->opaque)->currPos.itemIndex
bt 12
if $hits1 >= 4
disable 1
end
continue
end
set $hits2=0
break _bt_first if scan->indexRelation->rd_id == 16389
commands
silent
set $hits2=$hits2+1
printf "\n=== _bt_first hit=%d ===\n", $hits2
print scan->indexRelation->rd_id
print dir
print ((BTScanOpaqueData*)scan->opaque)->currPos.currPage
print ((BTScanOpaqueData*)scan->opaque)->currPos.firstItem
print ((BTScanOpaqueData*)scan->opaque)->currPos.lastItem
print ((BTScanOpaqueData*)scan->opaque)->currPos.itemIndex
bt 12
if $hits2 >= 1
disable 2
end
continue
end
set $hits3=0
break _bt_next if scan->indexRelation->rd_id == 16389
commands
silent
set $hits3=$hits3+1
printf "\n=== _bt_next hit=%d ===\n", $hits3
print scan->indexRelation->rd_id
print dir
print ((BTScanOpaqueData*)scan->opaque)->currPos.currPage
print ((BTScanOpaqueData*)scan->opaque)->currPos.firstItem
print ((BTScanOpaqueData*)scan->opaque)->currPos.lastItem
print ((BTScanOpaqueData*)scan->opaque)->currPos.itemIndex
bt 12
if $hits3 >= 3
disable 3
end
continue
end
shell touch /opt/db/pgcourse/quality/20261031T070355229225Z-1cdee0/index-user.ready
continue
quit
