# Row-level security check in the Fabric service (25 November 2026)

The semantic model `sm_mfg_ops` has two roles, defined in `src/mfgops/model_def.py`:

| Role | Filter |
|---|---|
| Plant access | `gold_dim_plant[plant_id] IN` the plants listed for `USERPRINCIPALNAME()` in `gold_security_plant_access` |
| All plants | none (group management, controlling) |

## Why the check is a DAX query and not "Test as role"

In the service, *Security → Plant access → Test as role* returns "Test as role does not work with Single Sign-On (SSO)". The Direct Lake model connects to the lakehouse with the viewer's identity (SSO). Testing a role as another user needs the model to use a fixed identity (a shared cloud connection), which this workspace does not have yet.

So the role filter was evaluated for each user in the security table with a DAX query in the DAX query view of the deployed model. The query applies exactly the role's filter expression with the user name substituted for `USERPRINCIPALNAME()`:

```dax
EVALUATE
ADDCOLUMNS (
    VALUES ( gold_security_plant_access[user_principal_name] ),
    "Visible plants",
        VAR u = gold_security_plant_access[user_principal_name]
        RETURN CONCATENATEX (
            FILTER ( ALL ( gold_dim_plant ), gold_dim_plant[plant_id] IN
                CALCULATETABLE ( VALUES ( gold_security_plant_access[plant_id] ),
                    ALL ( gold_security_plant_access ), gold_security_plant_access[user_principal_name] = u ) ),
            gold_dim_plant[plant_id], ", ", gold_dim_plant[plant_id] ),
    "Revenue CHF visible",
        VAR u = gold_security_plant_access[user_principal_name]
        RETURN CALCULATE ( [Revenue CHF], ALL ( gold_security_plant_access ),
            FILTER ( ALL ( gold_dim_plant ), gold_dim_plant[plant_id] IN
                CALCULATETABLE ( VALUES ( gold_security_plant_access[plant_id] ),
                    ALL ( gold_security_plant_access ), gold_security_plant_access[user_principal_name] = u ) ) ),
    "Revenue CHF all plants", CALCULATE ( [Revenue CHF], ALL ( gold_security_plant_access ) )
)
ORDER BY gold_security_plant_access[user_principal_name]
```

## Result

| User (fictional) | Visible plants | Revenue CHF visible | Revenue CHF all plants |
|---|---|---|---|
| ops.director@nordhalde.example | CH01, MY01, RO01 | 1,815,619,554.91 | 1,815,619,554.91 |
| plant.manager.ch01@nordhalde.example | CH01 | 806,444,871.90 | 1,815,619,554.91 |
| plant.manager.my01@nordhalde.example | MY01 | 642,322,179.66 | 1,815,619,554.91 |
| plant.manager.ro01@nordhalde.example | RO01 | 366,852,503.35 | 1,815,619,554.91 |

Each plant manager sees one plant, the director sees all three, and the three plant totals add up to the group total (806.4M + 642.3M + 366.9M = 1,815.6M), so no rows are lost or double-counted by the filter.

## Not yet done

- Assigning real Entra ID users or groups to the roles (the users above are fictional).
- Testing as a user end to end, which needs a fixed-identity connection for the Direct Lake model.
