/**
 *    Copyright 2009-2018 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.submitted.primitive_byte_array_result_type;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Reader;
import java.lang.reflect.Array;
import java.sql.Connection;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author Weicong Huang
 */
public class PrimitiveByteArrayResultTest {

  private static SqlSessionFactory sqlSessionFactory;

  @BeforeClass
  public static void setUp() throws Exception {
    // create an SqlSessionFactory
    Reader reader = Resources.getResourceAsReader("org/apache/ibatis/submitted/" +
      "primitive_byte_array_result_type/mybatis-config.xml");
    sqlSessionFactory = new SqlSessionFactoryBuilder().build(reader);
    reader.close();

    // populate in-memory database
    SqlSession session = sqlSessionFactory.openSession();
    Connection conn = session.getConnection();
    reader = Resources.getResourceAsReader("org/apache/ibatis/submitted/primitive_byte_array_result_type/CreateDB.sql");
    ScriptRunner runner = new ScriptRunner(conn);
    runner.setLogWriter(null);
    runner.runScript(reader);
    conn.close();
    reader.close();
    session.close();
  }

  @Test
  public void shouldGetPrimitiveArrayWithBinary() {
    SqlSession sqlSession = sqlSessionFactory.openSession();
    try {
      Mapper mapper = sqlSession.getMapper(Mapper.class);

      byte[] bin1 = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
      byte[] bin2 = new byte[] {10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
      byte[] varBin1 = new byte[] {1, 2, 3, 4, 5};
      byte[] varBin2 = new byte[] {10, 9, 8, 7, 6};

      int rows = mapper.insert(1, bin1, varBin1);
      assertEquals(1, rows);

      rows = mapper.insert(2, bin2, varBin2);
      assertEquals(1, rows);

      assertArrayEquals(bin1, mapper.getBin(1));
      assertArrayEquals(bin2, mapper.getBin(2));
      assertEquals(0, Array.getLength(mapper.getBin(3)));
    } finally {
      sqlSession.close();
    }
  }

  @Test
  public void shouldGetPrimitiveArrayWithVarBinary() {
    SqlSession sqlSession = sqlSessionFactory.openSession();
    try {
      Mapper mapper = sqlSession.getMapper(Mapper.class);

      byte[] bin1 = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
      byte[] bin2 = new byte[] {10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
      byte[] varBin1 = new byte[] {1, 2, 3, 4, 5};
      byte[] varBin2 = new byte[] {10, 9, 8, 7, 6};

      int rows = mapper.insert(1, bin1, varBin1);
      assertEquals(1, rows);

      rows = mapper.insert(2, bin2, varBin2);
      assertEquals(1, rows);

      assertArrayEquals(varBin1, mapper.getVarBin(1));
      assertArrayEquals(varBin2, mapper.getVarBin(2));
      assertEquals(0, Array.getLength(mapper.getVarBin(3)));
    } finally {
      sqlSession.close();
    }
  }

  @Test
  public void shouldGetMultiplePrimitiveArrayWithBinary() {
    SqlSession sqlSession = sqlSessionFactory.openSession();
    try {
      Mapper mapper = sqlSession.getMapper(Mapper.class);

      byte[] bin1 = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
      byte[] bin2 = new byte[] {10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
      byte[] varBin1 = new byte[] {1, 2, 3, 4, 5};
      byte[] varBin2 = new byte[] {10, 9, 8, 7, 6};

      int rows = mapper.insert(1, bin1, varBin1);
      assertEquals(1, rows);

      rows = mapper.insert(2, bin2, varBin2);
      assertEquals(1, rows);

      byte[][] bins = mapper.getAllBin();
      assertEquals(2, bins.length);

      assertArrayEquals(bin1, bins[0]);
      assertArrayEquals(bin2, bins[1]);
    } finally {
      sqlSession.close();
    }
  }

  @Test
  public void shouldGetMultiplePrimitiveArrayWithVarBinary() {
    SqlSession sqlSession = sqlSessionFactory.openSession();
    try {
      Mapper mapper = sqlSession.getMapper(Mapper.class);

      byte[] bin1 = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
      byte[] bin2 = new byte[] {10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
      byte[] varBin1 = new byte[] {1, 2, 3, 4, 5};
      byte[] varBin2 = new byte[] {10, 9, 8, 7, 6};

      int rows = mapper.insert(1, bin1, varBin1);
      assertEquals(1, rows);

      rows = mapper.insert(2, bin2, varBin2);
      assertEquals(1, rows);

      byte[][] varBins = mapper.getAllVarBin();
      assertEquals(2, varBins.length);

      assertArrayEquals(varBin1, varBins[0]);
      assertArrayEquals(varBin2, varBins[1]);
    } finally {
      sqlSession.close();
    }
  }

}
