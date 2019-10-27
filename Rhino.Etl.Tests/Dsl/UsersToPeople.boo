import Rhino.Etl.Tests.Integration

process UsersToPeople:
    input "test", Command = "SELECT id, name, email  FROM Users"
    SplitName()
    output "test", Command = """
        INSERT INTO People (UserId, FirstName, LastName, Email) 
        VALUES (@UserId, @FirstName, @LastName, @Email)
        """:
        row.UserId = row.Id
        
