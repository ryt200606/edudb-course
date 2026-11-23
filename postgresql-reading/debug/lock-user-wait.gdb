set pagination off
set confirm off
set breakpoint pending off
set print pretty off
set print elements 16
directory /opt/db/pgcourse/build/postgresql-14.11
set $hits1=0
break ProcSleep
commands
silent
set $hits1=$hits1+1
printf "\n=== ProcSleep hit=%d ===\n", $hits1
print locallock->tag
print locallock->lock->nRequested
print locallock->lock->nGranted
bt 12
if $hits1 >= 1
disable 1
end
continue
end
shell touch /opt/db/pgcourse/quality/20261031T070355229225Z-1cdee0/lock-user-wait.ready
continue
quit
