package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
//        Shop shop = queryWithPassThrough(id);
        //id2 -> getById(id2) 等同于 this::getById
        //工具类方式
        Shop shop = cacheClient
                .queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);

        //逻辑过期解决缓存击穿
//        Shop shop = queryWithLogicalExpire(id);
        //工具类方式
//        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY,
//                LOCK_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.SECONDS);

        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

//    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

//    public Shop queryWithLogicalExpire(Long id) {
//        //从redis中查缓存
//        //存在直接返回
//        String shopJson = stringRedisTemplate.opsForValue().get("cache:shop:" + id);
//        //未命中
//        if (StrUtil.isBlank(shopJson)) {
//            return null;
//        }
//        //命中，先把JSON反序列化取出对象
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//        //是否过期
//        if(expireTime.isAfter(LocalDateTime.now())){
//            //未过期
//            return shop;
//        }
//        //已过期，缓存重建
//        //获取互斥锁
//        String lockKey = LOCK_SHOP_KEY + id;
//        boolean isLock = tryLock(lockKey);
//        if(isLock){
//            //获取成功开启独立显存缓存重建
//            CACHE_REBUILD_EXECUTOR.submit(() -> {
//                //重建缓存，测试用20s
//                try {
//                    this.saveShop2Redis(id, 20L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                } finally {
//                    //释放锁
//                    unlock(lockKey);
//                }
//            });
//        }
//        //返回过期商店信息
//        return shop;
//    }

//    public Shop queryWithMutex(Long id) {
//        //从redis中查缓存
//        //存在直接返回
//        String shopJson = stringRedisTemplate.opsForValue().get("cache:shop:" + id);
//        if (StrUtil.isNotBlank(shopJson)) {
//            return JSONUtil.toBean(shopJson, Shop.class);
//        }
//        //判断命中的是否是空值
//        if (shopJson != null) {
//            return null;
//        }
//        //缓存重建
//        //获取自定义的互斥锁
//        String lockKey = "lock:shop:" + id;
//        Shop shop = null;
//        try {
//            boolean isLock = tryLock(lockKey);
////        判断是否成功获取互斥锁
//            while (!isLock) {
//                //        失败则休眠并重试
//                Thread.sleep(50);
//                isLock = tryLock(lockKey);
//            }
//            //获取成功后重查缓存
//            shopJson = stringRedisTemplate.opsForValue().get("cache:shop:" + id);
//            if (StrUtil.isNotBlank(shopJson)) {
//                return JSONUtil.toBean(shopJson, Shop.class);
//            }
//            //判断命中的是否是空值
//            if (shopJson != null) {
//                return null;
//            }
//
////        获取锁成功且缓存未命中，根据id查询数据库
//            shop = getById(id);
//            //模拟重建的延时
////            Thread.sleep(8000);
////        不存在返回错误
//            if (shop == null) {
//                //将空值存入redis
//                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
//
//                return null;
//            }
////        存在写入缓存并返回
//            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        } finally {
//            //        释放互斥锁
//            unlock(lockKey);
//        }
//
//
//        return shop;
//    }

//    public Shop queryWithPassThrough(Long id) {
//        //从redis中查缓存
//        //存在直接返回
//        String shopJson = stringRedisTemplate.opsForValue().get("cache:shop:" + id);
//        if (StrUtil.isNotBlank(shopJson)) {
//            return JSONUtil.toBean(shopJson, Shop.class);
//        }
//        //判断命中的是否是空值
//        if (shopJson != null) {
//            return null;
//        }
//
////        不存在，根据id查询数据库
//        Shop shop = getById(id);
////        不存在返回错误
//        if (shop == null) {
//            //将空值存入redis
//            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
//
//            return null;
//        }
////        存在写入缓存并返回
//        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        return shop;
//    }

//    private boolean tryLock(String key) {
//        //不存在则set，互斥锁核心逻辑
//        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
//        //只有true时才为true，因为包装类拆箱过程如果为null会出现空指针报错
//        return BooleanUtil.isTrue(flag);
//    }
//
//    private void unlock(String key) {
//        stringRedisTemplate.delete(key);
//    }

    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        Shop shop = getById(id);
        //测试
//        Thread.sleep(200);
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));

        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    @Override
    @Transactional
    //开启事务，缓存和数据库一致性
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        //先更新数据库再删除缓存
        updateById(shop);
        stringRedisTemplate.delete("cache:shop:" + id);
        return Result.ok();
    }
}
