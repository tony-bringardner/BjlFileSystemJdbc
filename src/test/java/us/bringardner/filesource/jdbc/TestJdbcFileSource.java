/**
 * <PRE>
 * 
 * Copyright Tony Bringarder 1998, 2025 
 * 
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       <A href="http://www.apache.org/licenses/LICENSE-2.0">http://www.apache.org/licenses/LICENSE-2.0</A>
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *  </PRE>
 *   
 *   
 *	@author Tony Bringardner   
 *
 *
 * ~version~V001.01.47-V000.01.28-V000.01.25-V000.01.23-V000.01.22-V000.01.18-V000.01.10-V000.01.06-V000.01.02-V000.01.01-V000.00.05-V000.00.03-V000.00.01-V000.00.00-
 */
package us.bringardner.filesource.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;
import java.util.Set;

import org.hsqldb.Server;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import us.bringardner.io.filesource.FileSource;
import us.bringardner.io.filesource.FileSourceFactory;
import us.bringardner.io.filesource.jdbcfile.JdbcFileSourceFactory;


@TestMethodOrder(OrderAnnotation.class)
public class TestJdbcFileSource extends FileSourceAbstractTestClass {

	
	private static Server server;
	private static String databaseName0 = "mainDb";
	private static String databasePath0 =  "mem:mainDb";
	private static String databaseName1 =  "standbyDb";
	private static String databasePath1 =  "mem:standbyDb";
	private static int databasePort = 9001;
	private static String databaseUrl ="jdbc:hsqldb:hsql://localhost:9001/mainDb";
	private static String databaseDriverName = "org.hsqldb.jdbc.JDBCDriver";
	private static int timeout = 4000;
	private static String databaseUser = "SA";
	private static String databasePassword = "";
	


	private boolean enableViewer = false;

	@BeforeAll
	public static void setUpBeforeClass() throws Exception {

		localTestFileDirPath = "TestFiles";
		localCacheDirPath = "target/TestFiles";
		remoteTestFileDirPath = "TestFiles";
		
		startHSQLDB();

		factory = FileSourceFactory.getFileSourceFactory(JdbcFileSourceFactory.FACTORY_ID);
		Properties p = factory.getConnectProperties();

		p.setProperty(JdbcFileSourceFactory.JDBC_DRIVER, databaseDriverName);
		p.setProperty(JdbcFileSourceFactory.JDBC_URL, databaseUrl);
		p.setProperty(JdbcFileSourceFactory.JDBC_USERID, databaseUser);
		p.setProperty(JdbcFileSourceFactory.JDBC_PASSWORD, databasePassword);

		assertTrue(factory.connect(p));
		try(InputStream in = TestJdbcFileSource.class.getResourceAsStream("/Hsqldb.ddl")) {
			String ddl = new String(in.readAllBytes());
			try(Connection con = ((JdbcFileSourceFactory)factory).getConnection()) {
				try(Statement stmt = con.createStatement()) {
					stmt.executeUpdate(ddl);					
				}
			}
		}
		
		
		/*
		org.hsqldb.util.DatabaseManagerSwing.main(new String[0]);
		
		System.out.println("Waiting for enter pressed");
		System.in.read();
		
		while(true) {
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				// Not implemented
				e.printStackTrace();
			}
		}
		*/

	}

	private static void startHSQLDB() throws Exception {
		server = new Server();
		// turn off HSQLDB logging
		server.setLogWriter(new PrintWriter(new OutputStream() {

			@Override
			public void write(int b) throws IOException {


			}
		}));

		server.setSilent(true);
		server.setDatabaseName(0, databaseName0);
		server.setDatabasePath(0, databasePath0);
		server.setDatabaseName(1, databaseName1);
		server.setDatabasePath(1,databasePath1);
		server.setPort(databasePort); 

		server.start();

		long start = System.currentTimeMillis();

		while(System.currentTimeMillis()-start < timeout && server.isNotRunning()) {
			Thread.sleep(100);
		}

		assertTrue(!server.isNotRunning()," Hsqldb did not start within timeout");
		Class.forName(databaseDriverName);
		if( verbose) System.out.println("HsqlServer is running");
		DriverManager.getConnection(databaseUrl, databaseUser, databasePassword);
		if( verbose) System.out.println("HsqlServer connection created");
		
		/*
		org.hsqldb.util.DatabaseManagerSwing.main(new String[0]);
		while(true) {
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				// Not implemented
				e.printStackTrace();
			}
		}
		*/
		
		
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
		if( factory != null){
			factory.disConnect();
			long start = System.currentTimeMillis();
			do {
				Thread.sleep(100);
			} while(!factory.isConnected() && System.currentTimeMillis()-start < 10000);
		}
		if( server != null) {
			server.stop();
			long start = System.currentTimeMillis();
			do {
				Thread.sleep(100);
			} while(!server.isNotRunning() && System.currentTimeMillis()-start < 10000);
		}

	}

	

	
	
	@Test
	@Order(13)
	public void testAppend() throws IOException {
		
		
		
		FileSource remoteDir = factory.createFileSource(remoteTestFileDirPath);
		assertTrue(remoteDir.mkdirs(),"Could not create remote testDir");
		byte [] data = "happy\n".getBytes();
		FileSource file = remoteDir.getChild("Happy.txt");
		OutputStream out = file.getOutputStream();
			for(int idx=0; idx< 10; idx++ ) {
				out.write(data);
			}						
		out.close();
		enableViewer=true;
		
		//pauseAndExamine(remoteDir);
		long len1 = file.length();
		assertEquals(data.length*10, len1,"First length wrong");
		
		out = file.getOutputStream(true);
			for(int idx=0; idx< 10; idx++ ) {
				out.write(data);
			}						
		out.close();
		long len2 = file.length();
		assertEquals(data.length*20, len2,"Second length wrong");
		
		assertTrue(file.delete(),"Could not delete "+file);
		
	}

	
	@Test()
	@Order(10)
	public void test2() throws Exception {
		
		
		
		FileSource localTestDir = FileSourceFactory.getDefaultFactory().createFileSource(localTestFileDirPath);
		assertTrue( localTestDir.exists(),"Local test dir does not exist");
		assertTrue(localTestDir.isDirectory(),"Local test dir is not a dir");
		FileSource remoteDir = factory.createFileSource(remoteTestFileDirPath);
		assertTrue(remoteDir.mkdirs(),"Could not create remote testDir");
		if( verbose) System.out.println("Remote dir created");

		
		// get the files
		FileSource[] list = localTestDir.listFiles();

		//  copy each to the remote system
		for (FileSource src : list) {
			if( src.isFile()) {				
				copy(src,remoteDir.getChild(src.getName()));
			}
		}
		if( verbose) System.out.println("Copy done");
		pauseAndExamine(remoteDir);
		
		//  Validate everything
		for (FileSource src : list) {
			if( src.isFile()) {
				String name2 = src.getName();
				compare(name2,src,remoteDir.getChild(name2));
			}
		}
		if( verbose) System.out.println("Compare done");

		


		//  get the list from the remote factory
		list = remoteDir.listFiles();

		//  Remove them
		for (FileSource src : list) {
			assertTrue(src.delete(),"Can't delete the test directory.");
		}

		list = remoteDir.listFiles();
		if( verbose) System.out.println("Delete files done...  list2="+list.length);

		if( list.length > 0 ) {
			//.fuse_hidden
			//  unfortunate bug in ubuntu that creates a hidden file when you delete from a directory
		} else { 
			// now remove the remote test directory
			boolean done = remoteDir.delete();
			assertTrue(done,"Can't delete the test directory.");
		}
		if( verbose) System.out.println("All delete done");



	}


	private void pauseAndExamine(FileSource dir) throws IOException {
		if( enableViewer) {
			org.hsqldb.util.DatabaseManagerSwing.main(new String[0]);
			while(enableViewer) {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					// Not implemented
					e.printStackTrace();
				}
			}
			/*
			if( dir != null && dir.exists()) {
				FileSourceFactory.setDefaultFactory(dir.getFileSourceFactory());
				FileSourceChooserDialog d = new FileSourceChooserDialog();
				d.setModal(true);
				d.openDialogAsBrowser();
				if( verbose) System.out.println("Browsing complete");
			}
			*/
		}
	}

	@Test
	@Order(12)
	public void testThree() throws Exception {
		
		 //user.name
		// u=rwx,g=rx,o=rx				022
		//System.getProperties().list(System.out);
		//File file = new File(testFileDir).getCanonicalFile();
		File file = File.createTempFile("prefix", "suffix");
		FileOutputStream out = new FileOutputStream(file);
		out.write("Hello\n".getBytes());
		out.close();
		PosixFileAttributeView view2 = Files.getFileAttributeView(file.toPath(), PosixFileAttributeView.class,LinkOption.NOFOLLOW_LINKS);
		if (view2 != null) {
			PosixFileAttributes at = view2.readAttributes();
			at.creationTime();
			at.fileKey();
			at.group();
			at.owner();
			Set<PosixFilePermission> perm = at.permissions();
			for(PosixFilePermission p : perm) {
				switch (p) {
				case OWNER_READ:
				case OWNER_WRITE:
				case OWNER_EXECUTE:
				case OTHERS_READ:
				case OTHERS_WRITE:
				case OTHERS_EXECUTE:
				case GROUP_READ:
				case GROUP_WRITE:
				case GROUP_EXECUTE:
					
					break;
				default:
					throw new IllegalArgumentException("Unexpected value: " + p);
				}
				
			}
		}
		UserPrincipal owner = java.nio.file.Files.getOwner(file.toPath());
		if( verbose) System.out.println("owner="+owner);
		if( verbose) System.out.println(""+file.canExecute()+" "+file.canRead()+" "+file.canWrite());
		file.delete();
	}


	


}
