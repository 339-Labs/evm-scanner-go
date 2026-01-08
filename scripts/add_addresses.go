//go:build ignore
// +build ignore

package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/redis/go-redis/v9"
)

// 用于批量添加内部地址到Redis Bloom过滤器的工具

var (
	redisAddr = flag.String("redis", "127.0.0.1:6379", "Redis地址")
	redisDB   = flag.Int("db", 0, "Redis数据库")
	redisPwd  = flag.String("password", "", "Redis密码")
	bloomKey  = flag.String("key", "address:bloom", "Bloom过滤器Key")
	inputFile = flag.String("file", "", "包含地址的文件，每行一个地址")
	useBloom  = flag.Bool("bloom", true, "使用Bloom过滤器（false则使用SET）")
)

func main() {
	flag.Parse()

	if *inputFile == "" {
		fmt.Println("请提供地址文件: -file addresses.txt")
		os.Exit(1)
	}

	// 连接Redis
	client := redis.NewClient(&redis.Options{
		Addr:     *redisAddr,
		Password: *redisPwd,
		DB:       *redisDB,
	})

	ctx := context.Background()
	if _, err := client.Ping(ctx).Result(); err != nil {
		fmt.Printf("连接Redis失败: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	// 读取地址文件
	file, err := os.Open(*inputFile)
	if err != nil {
		fmt.Printf("打开文件失败: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	var addresses []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		addr := strings.TrimSpace(scanner.Text())
		if addr != "" && strings.HasPrefix(addr, "0x") {
			addresses = append(addresses, strings.ToLower(addr))
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("读取文件失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("读取到 %d 个地址\n", len(addresses))

	if *useBloom {
		// 使用Bloom过滤器
		// 先创建Bloom过滤器
		_, err := client.Do(ctx, "BF.RESERVE", *bloomKey, 0.001, 10000000).Result()
		if err != nil {
			// 可能已存在，忽略错误
			fmt.Printf("创建Bloom过滤器: %v (可能已存在)\n", err)
		}

		// 批量添加
		batchSize := 1000
		for i := 0; i < len(addresses); i += batchSize {
			end := i + batchSize
			if end > len(addresses) {
				end = len(addresses)
			}

			batch := addresses[i:end]
			args := make([]interface{}, len(batch)+2)
			args[0] = "BF.MADD"
			args[1] = *bloomKey
			for j, addr := range batch {
				args[j+2] = addr
			}

			if _, err := client.Do(ctx, args...).Result(); err != nil {
				fmt.Printf("添加地址失败: %v\n", err)
				os.Exit(1)
			}

			fmt.Printf("已添加 %d/%d 个地址\n", end, len(addresses))
		}
	} else {
		// 使用SET
		members := make([]interface{}, len(addresses))
		for i, addr := range addresses {
			members[i] = addr
		}

		if err := client.SAdd(ctx, *bloomKey, members...).Err(); err != nil {
			fmt.Printf("添加地址失败: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Println("地址添加完成!")
}
