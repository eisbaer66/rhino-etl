import Rhino.Etl.Tests.Integration
 
process UsersToPeopleBulk:
    input "test", Command = "SELECT id, name, email  FROM Users"
    SplitName()
    sqlBulkInsert "test", "People", TableLock = true :
        map "id", int
        map "firstname"
        map "lastname"
        map "email"
        map "userid", "id", int
