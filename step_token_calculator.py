import asyncio
import aiohttp
import base64
import os
from pathlib import Path
import json
from datetime import datetime
from tqdm import tqdm
import random
import time

API_KEY = os.getenv("STEP_API_KEY")

async def encode_image_to_base64(image_path):
    with open(image_path, 'rb') as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

async def calculate_tokens(session, image_base64, api_key, model):
    url = "https://api.stepfun.com/v1/token/count"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {API_KEY}"
    }
    
    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": """请详细描述这张科学教学示意图。要求：

1. 描述长度：控制在100-300字之间

2. 描述结构（请按以下顺序组织内容）：
   A. 开篇概述（15-25字）
   B. 核心内容（50-150字）
   C. 教学功能（20-50字）
   D. 补充说明（如有必要，15-75字）

3. 描述原则：
   - 使用专业准确的生物学术语
   - 保持逻辑性和连贯性
   - 由表及里，由简到繁
   - 注重概念间的关联性"""
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "描述这张图片"
                    },
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{image_base64}"
                        }
                    }
                ]
            }
        ]
    }

    async with session.post(url, headers=headers, json=payload) as response:
        if response.status != 200:
            raise Exception(f"API请求失败: HTTP {response.status}")
            
        content_type = response.headers.get('Content-Type', '')
        if 'application/json' not in content_type:
            text = await response.text()
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                raise Exception(f"无法解析响应内容: {text[:200]}...")
                
        return await response.json()

async def process_single_image(image_file, session, models, results, log_files, api_key):
    try:
        image_base64 = await encode_image_to_base64(image_file)
        tasks = []
        for model in models:
            tasks.append(process_single_model(
                session, image_base64, api_key, model, 
                image_file, results, log_files
            ))
        await asyncio.gather(*tasks)
    except Exception as e:
        print(f"读取图片 {image_file.name} 失败: {str(e)}")

async def process_single_model(session, image_base64, api_key, model, image_file, results, log_files):
    max_retries = 2
    for attempt in range(max_retries):
        try:
            result = await calculate_tokens(session, image_base64, api_key, model)
            tokens = result['data']['total_tokens']
            results[model] += tokens
            log_message = f"图片 {image_file.name}: {tokens} tokens\n"
            log_files[model].write(log_message)
            print(f"图片 {image_file.name} 在 {model} 模型下的token数: {tokens}")
            break
        except Exception as e:
            if attempt == max_retries - 1:
                error_message = f"处理图片 {image_file.name} 失败 (重试{max_retries}次): {str(e)}\n"
                log_files[model].write(error_message)
                print(error_message)
            else:
                print(f"重试 {attempt + 1}/{max_retries}...")

async def process_images(folder_path, api_key):
    models = ["step-1v-8k", "step-1.5v-mini"]
    results = {model: 0 for model in models}
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_files = {
        model: open(f"/root/rawdata/{model}_tokens_{timestamp}.log", "w") 
        for model in models
    }
    summary_file = open(f"/root/rawdata/summary_{timestamp}.log", "w")
    
    # 获取所有图片文件
    image_files = list(Path(folder_path).glob('*.png'))
    total_files = len(image_files)
    print(f"共发现 {total_files} 个PNG文件")
    
    batch_size = 100
    processed = 0
    
    async with aiohttp.ClientSession() as session:
        while processed < total_files:
            # 获取当前批次的图片
            current_batch = image_files[processed:processed + batch_size]
            tasks = []
            
            # 创建当前批次的任务
            for image_file in current_batch:
                tasks.append(process_single_image(
                    image_file, session, models, results, log_files, api_key
                ))
            
            # 处理当前批次
            for f in tqdm(
                asyncio.as_completed(tasks), 
                total=len(tasks), 
                desc=f"处理批次 {processed//batch_size + 1}"
            ):
                await f
            
            processed += len(current_batch)
            
            # 如果还有未处理的图片，等待
            if processed < total_files:
                wait_time = random.uniform(31, 32)
                print(f"\n已处理 {processed}/{total_files} 个文件，等待{wait_time:.2f}秒...")
                await asyncio.sleep(wait_time)
    
    summary_file.write("总计token数:\n")
    for model, tokens in results.items():
        summary_file.write(f"{model}: {tokens}\n")
    
    for file in log_files.values():
        file.close()
    summary_file.close()
    
    return results

async def main():
    api_key = "7gYKNn5kdq2KXM0wGq3caI0Pr01kLdtuCSaph4j7kqsDios8Cwoq5KMv9Wajz1lT"
    if not api_key:
        raise ValueError("请设置环境变量 STEP_API_KEY")

    folder_path = "/root/rawdata/test_output"
    results = await process_images(folder_path, api_key)
    print("\n总计token数:")
    for model, tokens in results.items():
        print(f"{model}: {tokens}")

if __name__ == "__main__":
    asyncio.run(main())