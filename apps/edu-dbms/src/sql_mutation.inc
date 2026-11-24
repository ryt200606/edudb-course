// Incremental DML; DDL may still rebuild table layout through write_table.
json mutate(Session& session, Statement& statement,const json& write_features) {
    auto graph=integrity_lock(session,statement.table,statement.kind,write_features);
    const bool protected_table=graph.constrained.count(statement.table)!=0;
    if(!protected_table&&!try_record_locks(session,statement))acquire(session, "table:" + statement.table, true);
    const auto metadata = table(session, statement.table, false);
    const auto columns = schema_for(metadata, statement.table);
    auto result = blank_result();
    std::size_t affected = 0;
    auto rules=bind_integrity(session,statement.table,metadata);
    std::vector<RemovedKey> removed;
    const bool referenced=std::any_of(graph.edges.begin(),graph.edges.end(),[&](const ForeignEdge& edge){return edge.parent==statement.table;});


    if (statement.kind == "INSERT") {
        std::vector<int> targets;
        if (statement.names.empty()) {
            for (std::size_t i = 0; i < metadata["columns"].size(); ++i) targets.push_back(int(i));
        } else {
            std::set<std::string> seen;
            for (const auto& name : statement.names) {
                if (!seen.insert(name).second) fail("DUPLICATE_COLUMN", "Column specified twice: " + name);
                targets.push_back(column_id(metadata, name));
            }
        }
        // Type/name validation is independent of row count and always happens.
        std::vector<json> pending;
        for (const auto& values : statement.values) {
            json row = json::array();
            for (const auto& column : metadata["columns"]) row.push_back(column["default"]);
            if (!values.empty()) {
                if (values.size() != targets.size()) fail("COLUMN_COUNT_MISMATCH", "INSERT column/value counts differ");
                for (std::size_t i = 0; i < values.size(); ++i) {
                    bind_expr(values[i], {});
                    row[targets[i]] = eval(values[i], json::array());
                }
            } else if (!statement.names.empty()) {
                fail("COLUMN_COUNT_MISMATCH", "DEFAULT VALUES cannot include a column list");
            }
            pending.push_back(validate_row(row, metadata["columns"]));
        }
        for (const auto& row : pending) {
            // Storage probes its persistent unique indexes. A failing statement
            // is rolled back by the implicit transaction or marks an explicit
            // transaction aborted, so no partial changes can be committed.
            storage.insert_row(session.tx, statement.table, row);
            ++affected;
        }
        for(const auto& row:pending)integrity_check_row(session,rules,row);
        result["plan"] = {{"node", "IncrementalInsert"}, {"table", statement.table},
                           {"constraint_check", "persistent unique index probes"}};
    } else {
        std::vector<std::pair<int, E>> setters;
        if (statement.kind == "UPDATE") {
            std::set<int> seen;
            for (const auto& assignment : statement.assignments) {
                int column = column_id(metadata, assignment.first);
                if (!seen.insert(column).second) fail("DUPLICATE_COLUMN", "Column assigned twice: " + assignment.first);
                bind_expr(assignment.second, columns);
                const auto& target = metadata["columns"][column]["type"].get_ref<const std::string&>();
                const auto& source = assignment.second->type;
                if (source != "NULL" && source != target && !(target == "REAL" && source == "INTEGER"))
                    fail("TYPE_ERROR", "Cannot assign " + source + " to " + target + " column " + assignment.first);
                setters.push_back({column, assignment.second});
            }
        } else if (statement.kind != "DELETE") fail("UNSUPPORTED_SQL", "Statement is not DML");
        bind_expr(statement.where, columns); require_boolean(statement.where);
        std::vector<E> predicates; conjuncts(statement.where, predicates);
        std::vector<int> all_columns;for(std::size_t i=0;i<columns.size();++i)all_columns.push_back(static_cast<int>(i));
        auto choice = choose_scan(metadata, {statement.table, statement.table,"",false}, columns, 0, predicates,all_columns);
        std::vector<StoredRow> matches_snapshot;
        std::size_t examined = 0;
        if (!choice.empty) {
            auto cursor = storage.scan(session.tx, statement.table, choice.spec);
            StoredRow row;
            while (cursor->next(row)) {
                ++examined;
                if (matches(statement.where, row.values)) matches_snapshot.push_back(row);
            }
        }
        // The cursor is destroyed and the qualifying RID set frozen before any
        // mutation. Updating the predicate/index key cannot revisit moved rows.
        std::vector<json> changed_rows;
        for (const auto& row : matches_snapshot) {
            if (statement.kind == "DELETE") {
                storage.erase_row(session.tx, statement.table, row.id);
                if(referenced)removed.push_back({statement.table,row.values,nullptr});
            }
            else {
                json replacement = row.values;
                for (const auto& setter : setters)
                    replacement[setter.first] = eval(setter.second, row.values);
                replacement=validate_row(replacement,metadata["columns"]);
                storage.update_row(session.tx,statement.table,row.id,replacement);
                if(referenced)removed.push_back({statement.table,row.values,replacement});
                if(!rules.checks.empty()||!rules.outgoing.empty())changed_rows.push_back(std::move(replacement));
            }
            ++affected;
        }
        for(const auto& row:changed_rows)integrity_check_row(session,rules,row);
        if(!removed.empty()){
            auto cascaded=integrity_parent_changes(session,graph,removed);
            if(!cascaded.empty())result["cascade_tables"]=cascaded;
        }
        result["plan"] = {{"node", statement.kind == "UPDATE" ? "IncrementalUpdate" : "IncrementalDelete"},
                           {"source", choice.description}, {"halloween_protection", "frozen qualifying RID set"},
                           {"actual", {{"rows_examined", examined}, {"actual_rows", affected}, {"loops", 1}}}};
        result["metrics"]["rows_scanned"] = examined;
    }
    result["affected_rows"] = affected;
    result["metrics"]["actual_rows"] = affected;
    result["metrics"]["loops"] = 1;
    return result;
}
