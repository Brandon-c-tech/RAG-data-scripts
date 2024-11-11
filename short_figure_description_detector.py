import os
import json
import datetime

def find_json_files(directory):
    """递归查找所有以_figures_description.json结尾的文件"""
    json_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('_figures_description.json'):
                json_files.append(os.path.join(root, file))
    return json_files

def process_json_files(json_files):
    """处理所有json文件并收集短描述，同时删除这些条目和对应的图片"""
    short_descriptions = {}
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 找出所有值长度小于100的键值对
            short_items = {k: v for k, v in data.items() if len(v) < 100}
            
            if short_items:
                # 使用文件名作为键来组织数据
                filename = os.path.basename(json_file)
                short_descriptions[filename] = short_items
                
                # 从原始数据中删除这些条目
                for key in short_items:
                    del data[key]
                    # 删除对应的图片文件
                    figure_path = os.path.join(os.path.dirname(json_file), 'figures', key)
                    if os.path.exists(figure_path):
                        try:
                            os.remove(figure_path)
                            print(f"已删除图片: {figure_path}")
                        except Exception as e:
                            print(f"删除图片失败 {figure_path}: {str(e)}")
                
                # 保存更新后的json文件
                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                print(f"已更新json文件: {json_file}")
                
        except Exception as e:
            print(f"处理文件 {json_file} 时出错: {str(e)}")
    
    return short_descriptions

def save_results(results, output_file):
    """将结果保存到json文件"""
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"结果已保存到: {output_file}")
    except Exception as e:
        print(f"保存结果时出错: {str(e)}")

def main():
    # 设置要搜索的目录（当前目录）
    directory = "."
    
    # 设置输出路径
    output_dir = "/root/rawdata/batch_request"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 创建带时间戳的输出文件名
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"short_descriptions_{timestamp}.json")
    
    # 查找所有json文件
    json_files = find_json_files(directory)
    print(f"找到 {len(json_files)} 个json文件")
    
    # 处理文件并收集短描述，同时删除相关内容
    results = process_json_files(json_files)
    print(f"处理完成，共删除 {sum(len(v) for v in results.values())} 个短描述条目")
    
    # 保存结果
    save_results(results, output_file)

if __name__ == "__main__":
    main()