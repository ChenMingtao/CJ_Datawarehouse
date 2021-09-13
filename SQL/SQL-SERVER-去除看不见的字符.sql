USE cj_tprdb
GO
SET NOCOUNT ON
 DECLARE @TblName  VARCHAR(100)
 DECLARE @UpdateString NVARCHAR(1000)
 DECLARE @SelectString NVARCHAR(1000)
 DECLARE @COlName VARCHAR(100)
 DECLARE @COUNT  INT
 SET @TblName = 'test_varchar3'--指定想要修改的表名
 --定义游标取出指定表内的数据类型是VARCHAR,char,nVARCHAR的字段名称
 DECLARE cur_ColName  CURSOR
 FOR
 SELECT col.name
 FROM syscolumns AS col
 inner join sysobjects  AS obj  ON col.ID = obj.ID 
 INNER join systypes  AS typ  ON col.xtype = typ.xtype
 WHERE obj.xtype ='U'
 AND obj.name = @TblName
 AND typ.name IN ('VARCHAR','CHAR','NVARCHAR','NCHAR')
 FOR READ ONLY
 --打开游标
 OPEN cur_ColName
 FETCH NEXT FROM cur_ColName INTO @ColName
 IF @@FETCH_STATUS<>0
 BEGIN 
 PRINT '没有对应表或字段，'PRINT '请确认当前数据库内有' + @TblName + '表，' PRINT '或该表内有VARCHAR、CHAR、NVARCHAR、NCHAR类型的字段！' GOTO LABCLOSE
 END--循环修改
 WHILE @@FETCH_STATUS=0
 BEGIN 
 --拼修改字符串 
 --去掉左边的不可见字符 
 SET @SelectString = 'SELECT @COU=COUNT(*)     
 FROM ' + @TblName +'    
 WHERE ASCII(LEFT(' + @ColName +',1))<32
 AND '+ @ColName + ' IS NOT NULL' 
 EXEC sp_executesql @SelectString,N'@COU INT OUTPUT',
 @COUNT OUTPUT WHILE @COUNT>0 
 BEGIN  
 SET @UpdateString =   
 ' UPDATE ' + @TblName +   
 ' SET ' + @ColName + '=RIGHT(' + @ColName + ',LEN(' + @ColName + ')-1)    
 WHERE ASCII(LEFT(' + @ColName + ',1))<32    
 AND ' + @ColName + ' IS NOT NULL'  
 EXEC sp_executesql @UpdateString  
 EXEC sp_executesql @SelectString,N'@COU INT OUTPUT',@COUNT OUTPUT 
 END 
 --去掉右边的不可见字符 
 SET @SelectString = 'SELECT @COU=COUNT(*)     
 FROM ' + @TblName +'    
 WHERE ASCII(RIGHT(' + @ColName +',1))<32    
 AND '+ @ColName + ' IS NOT NULL' 
 EXEC sp_executesql @SelectString,N'@COU INT OUTPUT',
 @COUNT OUTPUT WHILE @COUNT>0 
 BEGIN  
 SET @UpdateString =   ' UPDATE ' + @TblName +   ' SET ' 
 + @ColName + '=LEFT(' + @ColName + ',LEN(' + @ColName + ')-1)    
 WHERE ASCII(RIGHT(' + @ColName + ',1))<32    
 AND ' + @ColName + ' IS NOT NULL'  
 EXEC SP_EXECUTESQL @UpdateString  
 EXEC sp_executesql @SelectString,N'@COU INT OUTPUT',
 @COUNT OUTPUT 
 END 
 PRINT 'column: ' + @ColName + '－－－ok' 
 FETCH NEXT FROM cur_ColName INTO @ColName
 END
 --关闭、释放游标
 LABCLOSE: CLOSE cur_ColName  
 DEALLOCATE cur_ColName
 GO
