set pagination off
set confirm off
set breakpoint pending off
set print pretty off
set print elements 16
directory /opt/db/pgcourse/build/postgresql-14.11
set $hits1=0
break RecordTransactionCommit
commands
silent
set $hits1=$hits1+1
printf "\n=== RecordTransactionCommit hit=%d ===\n", $hits1
print CurrentTransactionState->fullTransactionId.value
bt 12
if $hits1 >= 1
disable 1
end
continue
end
set $hits2=0
break XLogFlush
commands
silent
set $hits2=$hits2+1
printf "\n=== XLogFlush hit=%d ===\n", $hits2
print record
print CurrentTransactionState->fullTransactionId.value
bt 12
if $hits2 >= 1
disable 2
end
continue
end
shell touch /opt/db/pgcourse/quality/20261031T070355229225Z-1cdee0/wal-user-commit.ready
continue
quit
