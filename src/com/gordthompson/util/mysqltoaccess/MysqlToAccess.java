/*
 * Copyright 2016 Gordon D. Thompson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gordthompson.util.mysqltoaccess;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;

import org.apache.commons.io.FilenameUtils;

/**
 * A simple console application to dump the tables from a MySQL database 
 * into a newly-created Access database file.
 * <p>
 * Usage:
 * <p>
 * The path of the output file is provided as the first (and only)
 * command-line parameter, e.g., 
 * <p>
 * {@code java -jar MysqlToAccess.jar /home/gord/accessfile.accdb}
 * <p>
 * MySQL connection properties and other runtime options are controlled by a 
 * {@code MysqlToAccess.properties} file that resides in the same folder as 
 * the {@code main} class (or runnable JAR) file.
 * <p>
 * Dependencies:
 * <p>
 * This application uses MySQL Connector/J to read the MySQL database,
 * Jackcess to write the Access database file, and Apache Commons IO for
 * filespec juggling. See the Maven dependencies for more information.
 * 
 * @version 1.0.0
 * @author Gord Thompson
 *
 */
public class MysqlToAccess {

	public static void main(String[] args) {
		final String propFileName = "MysqlToAccess.properties";
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream(propFileName);
			prop.load(input);
		} catch (IOException ex) {
			ex.printStackTrace(System.err);
			System.exit(1);
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace(System.err);
					System.exit(1);
				}
			}
		}
		
		StringBuilder sb = new StringBuilder("jdbc:mysql://");
		sb.append((prop.getProperty("host") != null) ? prop.getProperty("host") : "localhost");
		sb.append(":");
		sb.append((prop.getProperty("port") != null) ? prop.getProperty("port") : "3306");
		sb.append("/");
		sb.append((prop.getProperty("database") != null) ? prop.getProperty("database") : "mysql");
		sb.append("?useUnicode=");
		sb.append((prop.getProperty("useUnicode") != null) ? prop.getProperty("useUnicode") : "true");
		sb.append("&characterEncoding=");
		sb.append((prop.getProperty("characterEncoding") != null) ? prop.getProperty("characterEncoding") : "UTF-8");
		String myConnectionString = sb.toString();
		
		Connection myConn = null;
		Database accessDb = null;
		try {
			myConn = DriverManager.getConnection(
					myConnectionString, 
					(prop.getProperty("user") != null) ? prop.getProperty("user") : "root", 
					(prop.getProperty("password") != null) ? prop.getProperty("password") : "");
			
			String accessFileSpec = "untitled.mdb";
			try {
				accessFileSpec = args[0];
			} catch (ArrayIndexOutOfBoundsException e) {
				// use default FileSpec
			}
			String fileSpecLCase = accessFileSpec.toLowerCase();
			String currentFileExtension =  FilenameUtils.getExtension(fileSpecLCase);
			if (!(currentFileExtension.equalsIgnoreCase("mdb") || currentFileExtension.equalsIgnoreCase("accdb"))) {
				accessFileSpec = FilenameUtils.removeExtension(accessFileSpec) + ".mdb";
				fileSpecLCase = accessFileSpec.toLowerCase();
				currentFileExtension =  "mdb";
			}
			Database.FileFormat fileFormat;
			if (prop.getProperty("fileFormat") == null) {
				if (currentFileExtension.equalsIgnoreCase("accdb")) {
					fileFormat = Database.FileFormat.valueOf("V2007");
				} else {
					fileFormat = Database.FileFormat.valueOf("V2000");
				}
			} else {
				fileFormat = Database.FileFormat.valueOf(prop.getProperty("fileFormat"));
				currentFileExtension =  fileFormat.getFileExtension().substring(1);
				accessFileSpec = FilenameUtils.removeExtension(accessFileSpec) + "." + currentFileExtension;
				fileSpecLCase = accessFileSpec.toLowerCase();
			}
			
			File accessFile = new File(accessFileSpec);
			if (accessFile.exists()) {
				if ("true".equalsIgnoreCase((prop.getProperty("overwriteExistingFile") != null) ? prop.getProperty("overwriteExistingFile") : "false")) {
					if (!accessFile.delete()) {
						System.err.println("Failed to delete existing Access database file.");
						myConn.close();
						System.exit(2);
					}
				} else {
					System.err.printf("Output file \"%s\" already exists.%n", accessFileSpec);
					System.err.printf("(To overwrite, use 'overwriteExistingFile=true' in %s.)%n", propFileName);
					myConn.close();
					System.exit(3);
				}
			}
			
			accessDb = new DatabaseBuilder(accessFile)
				    .setFileFormat(fileFormat)
				    .setAutoSync(false)
				    .create();

			DatabaseMetaData dmd = myConn.getMetaData();
			ResultSet rsTables = dmd.getTables(null, null, "%", new String[] {"TABLE"});
			while (rsTables.next()) {
				String tblName = rsTables.getString("TABLE_NAME");
				TableBuilder tblBuilder = new TableBuilder(tblName);
				ResultSet rsColumns = dmd.getColumns(null, null, tblName, "%");
				int colCount = 0;
				while (rsColumns.next()) {
					colCount++;
					String typeName = rsColumns.getString("TYPE_NAME");
					if (typeName.equals("INT")) {
						tblBuilder.addColumn(new ColumnBuilder(rsColumns.getString("COLUMN_NAME"), DataType.LONG));
					} else if (typeName.equals("DOUBLE")) {
						tblBuilder.addColumn(new ColumnBuilder(rsColumns.getString("COLUMN_NAME"), DataType.DOUBLE));
					} else if (typeName.equals("DECIMAL")) {
						tblBuilder.addColumn(new ColumnBuilder(rsColumns.getString("COLUMN_NAME"), DataType.NUMERIC)
								.setScale(Integer.parseInt(rsColumns.getString("DECIMAL_DIGITS")))
								.setPrecision(Integer.parseInt(rsColumns.getString("COLUMN_SIZE"))));
					} else if (typeName.equals("VARCHAR")) {
						tblBuilder.addColumn(new ColumnBuilder(rsColumns.getString("COLUMN_NAME"), DataType.TEXT)
								.setLengthInUnits(Integer.parseInt(rsColumns.getString("COLUMN_SIZE")))
								.setCompressedUnicode(true));
					} else if (typeName.equals("TEXT")) {
						tblBuilder.addColumn(new ColumnBuilder(rsColumns.getString("COLUMN_NAME"), DataType.MEMO)
								.setCompressedUnicode(true));
					} else if (typeName.equals("LONGBLOB")) {
						tblBuilder.addColumn(new ColumnBuilder(rsColumns.getString("COLUMN_NAME"), DataType.OLE));
					} else if (typeName.equals("BIT")) {
						tblBuilder.addColumn(new ColumnBuilder(rsColumns.getString("COLUMN_NAME"), DataType.BOOLEAN));
					} else if (Arrays.asList("DATETIME", "TIMESTAMP").contains(typeName)) {
						tblBuilder.addColumn(new ColumnBuilder(rsColumns.getString("COLUMN_NAME"), DataType.SHORT_DATE_TIME));
					} else {
						System.err.printf("Unknown MySQL column type %s for `%s`.`%s`%n", typeName, tblName, rsColumns.getString("COLUMN_NAME"));
						accessDb.close();
						myConn.close();
						System.exit(4);
					}
				}
				Table tbl = tblBuilder.toTable(accessDb);
				Statement s = myConn.createStatement();
				ResultSet rs = s.executeQuery("SELECT * FROM `" + tblName + "`");
				int rowCount = 0;
				while (rs.next()) {
					rowCount++;
					Object[] rowData = new Object[colCount];
					for (int i = 0; i < colCount; i++) {
						try {
							rowData[i] = rs.getObject(i+1);
						} catch (java.sql.SQLException se) {
							if (se.getMessage().equals("Value '0000-00-00 00:00:00' can not be represented as java.sql.Timestamp")) {
								rowData[i] = null;
							} else {
								System.err.printf("Error processing row %d in `%s`: %s%n", rowCount, tblName, se.getMessage());
							}
						}
					}
					tbl.addRow(rowData);
				}
				rs.close();
				s.close();
			}
		} catch (Exception e) {
			e.printStackTrace(System.err);
		} finally {
		    if (accessDb != null) {
		        try {
		            accessDb.close();
		        } catch (Exception e) {
		            e.printStackTrace(System.err);
		        }
		    }
		    if (myConn != null) {
		        try {
		            myConn.close();
		        } catch (Exception e) {
		            e.printStackTrace(System.err);
		        }
		    }
		}

	}

}
