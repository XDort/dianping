local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]

-- 该优惠券库存key
local stockKey = 'seckill:stock:' .. voucherId
-- 订单key看有哪些用户买了该优惠券，用的是set集合value多个不可重复
local orderKey = 'seckill:order:' .. voucherId
-- string转数字
if(tonumber(redis.call('get', stockKey)) <= 0 ) then
    -- 库存不足
    return 1
end
-- 判断集合中value是否存在 sismember orderKey userId
if(redis.call('sismember', orderKey, userId) == 1) then
    -- 存在重复下单
    return 2
end

-- 扣库存 incrby stockKey -1
redis.call('incrby', stockKey , -1)
-- 保存用户到集合中 sadd orderKey userId
redis.call('sadd', orderKey, userId)
-- 发送消息到队列中 XADD stream.orders * k1 v1 k2 v2 ...
redis.call('xadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)
return 0