import tiktoken
import os

def count_tokens_in_md(file_path, model="gpt-3.5-turbo"):
    """
    计算MD文件中的token数量
    
    Args:
        file_path: MD文件路径
        model: 使用的模型名称，默认为gpt-3.5-turbo
    
    Returns:
        token数量
    """
    try:
        # 创建编码器
        encoding = tiktoken.encoding_for_model(model)
        
        # 读取MD文件
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        # 计算tokens
        tokens = encoding.encode(content)
        token_count = len(tokens)
        
        # 输出结果
        print(f"\n文件: {os.path.basename(file_path)}")
        print(f"Token数量: {token_count}")
        print(f"预估字符数: {len(content)}")
        
        # 计算大约费用
        cost = (token_count / 1000000) * 0.02  # $0.02 per 1M tokens
        print(f"text-embedding-3-small预估费用: ${cost:.4f}")
        cost2 = (token_count / 1000000) * 0.13   # $0.13 per 1M tokens
        print(f"text-embedding-3-large预估费用: ${cost2:.4f}")
        
        return token_count
        
    except Exception as e:
        print(f"处理文件时出错: {str(e)}")
        return 0

if __name__ == "__main__":
    base_dir = "/root/rawdata/gcs/textbook_ocr"
    total_tokens = 0
    total_cost_small = 0
    total_cost_large = 0
    
    # 遍历目录
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith('.md'):
                file_path = os.path.join(root, file)
                print(f"\n处理文件: {file_path}")
                tokens = count_tokens_in_md(file_path)
                total_tokens += tokens
                total_cost_small += (tokens / 1000000) * 0.02
                total_cost_large += (tokens / 1000000) * 0.13
    
    print("\n总计:")
    print(f"总Token数: {total_tokens}")
    print(f"text-embedding-3-small总费用: ${total_cost_small:.4f}")
    print(f"text-embedding-3-large总费用: ${total_cost_large:.4f}")