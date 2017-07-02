package org.moshe.arad;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.moshe.arad.local.snapshot.SnapshotAPI;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SnapshotApiTest {

	@Autowired
	private RedisTemplate<String, String> redisTemplate;
	
	@Autowired
	private SnapshotAPI snapshotApi;
	
	@Before
	public void init(){
		redisTemplate.getConnectionFactory().getConnection().flushAll();
		int actual = redisTemplate.keys("*").size();
		assertEquals("Failed to initiate redis instance...", 0, actual);
	}
	
	@Test
	public void isLastUpdateDateExistsNotHaveKeyTest(){
		boolean result = redisTemplate.hasKey(SnapshotAPI.LAST_UPDATED);
		assertEquals("isLastUpdateDateExistsNotHaveKeyTest failed", false, result);
		
		boolean actual = snapshotApi.isLastUpdateDateExists();
		assertEquals("isLastUpdateDateExistsNotHaveKeyTest failed...", false, actual);
	}
	
	@Test
	public void isLastUpdateDateExistsHaveKeyTest(){
		boolean result;
		
		result = redisTemplate.hasKey(SnapshotAPI.LAST_UPDATED);
		assertEquals("isLastUpdateDateExistsHaveKeyTest failed...", false, result);
		
		redisTemplate.opsForValue().set(SnapshotAPI.LAST_UPDATED, new Date().toString());
		
		result = redisTemplate.hasKey(SnapshotAPI.LAST_UPDATED);
		assertEquals("isLastUpdateDateExistsHaveKeyTest failed...", true, result);
		
		boolean actual = snapshotApi.isLastUpdateDateExists();
		assertEquals("isLastUpdateDateExistsHaveKeyTest failed...", true, actual);
	}
	
	@Test
	public void saveLatestSnapshotDateTest(){
		boolean result = redisTemplate.hasKey(SnapshotAPI.LAST_UPDATED);
		assertEquals("isLastUpdateDateExistsNotHaveKeyTest failed", false, result);
		
		Date date = new Date(1983, 6, 8);
		snapshotApi.saveLatestSnapshotDate(date);
		
		boolean actual = redisTemplate.hasKey(SnapshotAPI.LAST_UPDATED);
		assertEquals("isLastUpdateDateExistsNotHaveKeyTest failed", true, actual);
	}
	
	@Test
	public void saveLatestSnapshotNullDateTest1(){
		boolean result = redisTemplate.hasKey(SnapshotAPI.LAST_UPDATED);
		assertEquals("isLastUpdateDateExistsNotHaveKeyTest failed", false, result);
		
		Date date = null;
		snapshotApi.saveLatestSnapshotDate(date);
		
		boolean actual = redisTemplate.hasKey(SnapshotAPI.LAST_UPDATED);
		assertEquals("isLastUpdateDateExistsNotHaveKeyTest failed", false, actual);
	}
	
	@Test
	public void getLastUpdateDateTest(){
		boolean result = redisTemplate.hasKey(SnapshotAPI.LAST_UPDATED);
		assertEquals("getLastUpdateDateTest failed", false, result);
		
		Date date = new Date(1983, 6, 8);
		snapshotApi.saveLatestSnapshotDate(date);
		
		boolean actual = redisTemplate.hasKey(SnapshotAPI.LAST_UPDATED);
		assertEquals("getLastUpdateDateTest failed", true, actual);
		
		Date actualDate = snapshotApi.getLastUpdateDate();
		assertEquals("getLastUpdateDateTest failed", date, actualDate);
	}
	
	@Test
	public void readLatestSnapshotHasNotLastUpdateDateTest(){
		boolean result = redisTemplate.hasKey(SnapshotAPI.LAST_UPDATED);
		assertEquals("getLastUpdateDateTest failed", false, result);
		
		assertNull("readLatestSnapshotHasNotLastUpdateDate failed", snapshotApi.readLatestSnapshot());
	}
	
	@Test(expected = RuntimeException.class)
	public void readLatestSnapshotHasUpdateDateEmptyUsersTest(){
		boolean result = redisTemplate.hasKey(SnapshotAPI.LAST_UPDATED);
		assertEquals("readLatestSnapshotHasUpdateDateEmptyUsers failed", false, result);
		
		Date date = new Date(1983, 6, 8);
		snapshotApi.saveLatestSnapshotDate(date);
		
		boolean actual = redisTemplate.hasKey(SnapshotAPI.LAST_UPDATED);
		assertEquals("readLatestSnapshotHasUpdateDateEmptyUsers failed", true, actual);
		
		snapshotApi.readLatestSnapshot();
	}
	
	@Test
	public void readLatestSnapshotHasUpdateDateWithStringUsersTest(){
		boolean result = redisTemplate.hasKey(SnapshotAPI.LAST_UPDATED);
		assertEquals("readLatestSnapshotHasUpdateDateWithStringUsers failed", false, result);
		
		Date date = new Date(1983, 6, 8);
		snapshotApi.saveLatestSnapshotDate(date);
		
		boolean actual = redisTemplate.hasKey(SnapshotAPI.LAST_UPDATED);
		assertEquals("readLatestSnapshotHasUpdateDateWithStringUsers failed", true, actual);
		
		redisTemplate.opsForHash().put(SnapshotAPI.USERS, "User1", "User1");
		redisTemplate.opsForHash().put(SnapshotAPI.USERS, "User2", "User2");
		
		Map<String,Map<Object, Object>> actualLatestSnapshot = snapshotApi.readLatestSnapshot();
		assertEquals("readLatestSnapshotHasUpdateDateWithStringUsers failed", 1, actualLatestSnapshot.size());
		assertEquals("readLatestSnapshotHasUpdateDateWithStringUsers failed", true, actualLatestSnapshot.keySet().contains(SnapshotAPI.USERS));
		assertEquals("readLatestSnapshotHasUpdateDateWithStringUsers failed", 2, actualLatestSnapshot.get(SnapshotAPI.USERS).size());
		assertEquals("readLatestSnapshotHasUpdateDateWithStringUsers failed", "User1", actualLatestSnapshot.get(SnapshotAPI.USERS).get("User1"));
		assertEquals("readLatestSnapshotHasUpdateDateWithStringUsers failed", "User2", actualLatestSnapshot.get(SnapshotAPI.USERS).get("User2"));
	}
	
	@Test
	public void updateLatestSnapshotTest(){
		Map<String,Map<Object, Object>> snapshot = new HashMap<>();
		snapshot.put(SnapshotAPI.USERS, new HashMap<Object,Object>());
		snapshot.get(SnapshotAPI.USERS).put("User1", "User1");
		snapshot.get(SnapshotAPI.USERS).put("User2", "User2");
		
		snapshotApi.updateLatestSnapshot(snapshot, new Date(1983, 6, 8));
		Map<String,Map<Object, Object>> actual = snapshotApi.readLatestSnapshot();
		
		assertEquals("updateLatestSnapshotTest failed...", snapshot, actual);
	}
}
