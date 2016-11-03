SELECT * FROM testData1, testData2 WHERE testData1.A = testData2.G;
SELECT data1.A, data2.G, data1.B, data2.H FROM testData1 data1, testData2 data2 WHERE data1.A = data2.G AND data1.B = data2.H;
SELECT DISTINCT * FROM testData1, testData2 WHERE testData1.A = testData2.G ORDER BY testData1.A;