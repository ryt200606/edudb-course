set pagination off
set confirm off
set breakpoint pending off
set print pretty off
set print elements 16
directory /opt/db/pgcourse/build/postgresql-14.11
set $hits1=0
break HeapTupleSatisfiesMVCC if htup->t_tableOid == 16392
commands
silent
set $hits1=$hits1+1
printf "\n=== HeapTupleSatisfiesMVCC hit=%d ===\n", $hits1
print htup->t_tableOid
print htup->t_self
print htup->t_data->t_choice.t_heap.t_xmin
print htup->t_data->t_choice.t_heap.t_xmax
print htup->t_data->t_infomask
print htup->t_data->t_infomask2
print snapshot->xmin
print snapshot->xmax
print snapshot->xcnt
print snapshot->curcid
bt 12
if $hits1 >= 6
disable 1
end
continue
end
shell touch /opt/db/pgcourse/quality/20261031T070355229225Z-1cdee0/mvcc-user.ready
continue
quit
