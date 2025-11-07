# -*- coding: utf-8 -*-
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import requests
import json
import sys
import traceback
import time

def validate_config():
    """验证配置"""
    webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=dff99b4e-b4f4-44a5-87aa-9cb326de8777"
    
    # 检查Webhook地址格式
    if not webhook_url.startswith('https://qyapi.weixin.qq.com/cgi-bin/webhook/send'):
        return False, "企业微信Webhook地址格式不正确"
    
    return True, "配置验证通过"

def send_wechat_message(content, webhook_url, max_retries=3):
    """发送消息到企业微信机器人，带有重试机制"""
    data = {
        "msgtype": "markdown_v2",
        "markdown_v2": {
            "content": content
        }
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    
    for attempt in range(max_retries):
        try:
            print(f"第{attempt+1}次尝试发送消息...")
            response = requests.post(webhook_url, data=json.dumps(data), headers=headers, timeout=15)
            
            if response.status_code == 200:
                result = response.json()
                if result.get('errcode') == 0:
                    print("✅ 消息已成功发送到企业微信群！")
                    return True
                else:
                    error_msg = f"企业微信接口返回错误: {result}"
                    print(f"❌ {error_msg}")
                    
                    # 如果是限流错误，等待后重试
                    if result.get('errcode') == 45009:  # API调用次数超限
                        wait_time = 60  # 等待1分钟
                        print(f"🔄 API限流，等待{wait_time}秒后重试...")
                        time.sleep(wait_time)
                        continue
            else:
                print(f"❌ 网络请求失败，状态码: {response.status_code}")
                
        except requests.exceptions.Timeout:
            print(f"❌ 第{attempt+1}次发送超时")
        except requests.exceptions.ConnectionError:
            print(f"❌ 第{attempt+1}次连接错误")
        except Exception as e:
            print(f"❌ 第{attempt+1}次发送异常: {e}")
        
        # 如果不是最后一次尝试，等待后重试
        if attempt < max_retries - 1:
            wait_time = (attempt + 1) * 10  # 重试等待时间递增
            print(f"🔄 等待{wait_time}秒后重试发送...")
            time.sleep(wait_time)
    
    print("❌ 消息发送失败，已超过最大重试次数")
    return False

def get_notice_data_safe(date_str, max_retries=2):
    """安全获取公告数据，带有完善的错误处理"""
    for attempt in range(max_retries):
        try:
            print(f"📡 第{attempt+1}次尝试获取 {date_str} 的公告...")
            start_time = time.time()
            
            # 关键修复：创建一个不使用代理的Session
            session = requests.Session()
            session.trust_env = False  # 这行代码会忽略系统代理设置
            # 将自定义session传递给akshare
            day_df = ak.stock_notice_report(date=date_str, session=session)
            elapsed_time = time.time() - start_time
            
            print(f"✅ 请求成功，耗时: {elapsed_time:.2f}秒")
            
            # 验证返回数据格式
            if day_df is None:
                print("❌ 接口返回None")
                continue
                
            if not isinstance(day_df, pd.DataFrame):
                print(f"❌ 返回数据类型错误: {type(day_df)}")
                continue
                
            if day_df.empty:
                print("ℹ️ 该日无公告数据")
                return day_df  # 空DataFrame是正常情况
                
            # 检查必要列是否存在
            required_columns = ['公告标题', '代码', '名称', '公告日期']
            missing_columns = [col for col in required_columns if col not in day_df.columns]
            if missing_columns:
                print(f"⚠️ 数据缺少必要列: {missing_columns}")
                print(f"ℹ️ 现有列: {list(day_df.columns)}")
                # 尝试继续处理，但记录警告
                
            print(f"📊 获取到 {len(day_df)} 条公告")
            return day_df
            
        except requests.exceptions.Timeout:
            print(f"⏰ 第{attempt+1}次请求超时")
        except requests.exceptions.ConnectionError as e:
            print(f"🔌 第{attempt+1}次连接错误: {e}")
        except Exception as e:
            error_msg = str(e)
            print(f"❌ 第{attempt+1}次获取失败: {error_msg}")
            
            # 针对特定错误类型处理
            if "HTTPSConnectionPool" in error_msg:
                print("🌐 网络连接问题，请检查网络设置")
            elif "certificate" in error_msg.lower():
                print("🔒 SSL证书验证失败")
        
        # 重试前等待
        if attempt < max_retries - 1:
            wait_time = (attempt + 1) * 5
            print(f"🔄 等待{wait_time}秒后重试...")
            time.sleep(wait_time)
    
    print(f"❌ 获取 {date_str} 数据失败，已超过最大重试次数")
    return None  # 返回None表示彻底失败

def create_fallback_message(display_date_str, end_time, error_type, error_details=""):
    """创建降级消息"""
    base_message = f"# 🏛️ 司法拍卖公告提示 \n\n**📊 统计时间：{display_date_str} 08:30 - {end_time.strftime('%Y年%m月%d日')} 08:30**\n\n"
    
    error_messages = {
        "network": f"**❌ 网络连接故障**\n\n**错误详情：数据源连接失败**\n\n**💡 建议：请检查网络连接，脚本将自动重试**\n\n",
        "data": f"**❌ 数据处理异常**\n\n**错误详情：{error_details}**\n\n**💡 建议：请联系管理员检查数据格式**\n\n",
        "config": f"**❌ 配置验证失败**\n\n**错误详情：{error_details}**\n\n**💡 建议：请检查Webhook配置**\n\n",
        "resource": f"**❌ 系统资源不足**\n\n**错误详情：{error_details}**\n\n**💡 建议：请清理系统资源**\n\n",
        "unknown": f"**❌ 未知错误发生**\n\n**错误详情：{error_details}**\n\n**💡 建议：请联系技术支持**\n\n"
    }
    
    return base_message + error_messages.get(error_type, error_messages["unknown"]) + f"**✅ 最后尝试时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}**"

def main():
    # 脚本开始时间
    script_start_time = time.time()
    max_script_runtime = 600  # 10分钟最大运行时间
    
    try:
        print("=" * 60)
        print("🏁 开始执行A股司法拍卖公告查询...")
        print("=" * 60)
        
        # 0. 配置验证
        print("🔧 验证配置...")
        config_valid, config_msg = validate_config()
        if not config_valid:
            error_message = create_fallback_message("", datetime.now(), "config", config_msg)
            send_wechat_message(error_message, "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=dff99b4e-b4f4-44a5-87aa-9cb326de8777")
            return False
        
        print("✅ 配置验证通过")
        
        # 1. 计算时间范围
        today = datetime.now()
        end_time = today.replace(hour=8, minute=30, second=0, microsecond=0)
        start_time = end_time - timedelta(days=1)

        start_date_str = start_time.strftime("%Y%m%d")
        end_date_str = end_time.strftime("%Y%m%d")
        display_date_str = start_time.strftime("%Y年%m月%d日")

        print(f"📅 查询时间范围: {start_time} 至 {end_time}")
        print(f"📅 查询日期: {start_date_str} 到 {end_date_str}")

        # 2. 获取公告数据
        print("\n📡 开始获取公告数据...")
        
        df_list = []
        data_fetch_success = True
        fetch_errors = []
        
        dates_to_fetch = [
            start_time.strftime("%Y%m%d"),
            (start_time + timedelta(days=1)).strftime("%Y%m%d")
        ]
        
        for date_str in dates_to_fetch:
            # 检查脚本运行时间，避免超时
            if time.time() - script_start_time > max_script_runtime:
                print("⏰ 脚本运行时间过长，提前结束")
                break
                
            day_df = get_notice_data_safe(date_str)
            
            if day_df is None:
                # 彻底失败
                data_fetch_success = False
                fetch_errors.append(f"日期 {date_str} 获取失败")
            elif not day_df.empty:
                df_list.append(day_df)
                print(f"✅ 日期 {date_str} 处理完成")
            else:
                print(f"ℹ️ 日期 {date_str} 无数据")
        
        # 3. 数据处理和筛选
        filtered_notices = pd.DataFrame()
        
        if data_fetch_success and df_list:
            try:
                all_notices_df = pd.concat(df_list, ignore_index=True)
                print(f"📊 合并后总公告数: {len(all_notices_df)} 条")
                
                # 筛选司法拍卖公告
                if '公告标题' in all_notices_df.columns:
                    filtered_notices = all_notices_df[
                        all_notices_df['公告标题'].str.contains('拍卖', na=False) & 
                        all_notices_df['公告标题'].str.contains('提示性', na=False)
                    ]
                    print(f"🎯 筛选出司法拍卖公告: {len(filtered_notices)} 条")
                else:
                    print("⚠️ 数据中缺少'公告标题'列")
                    data_fetch_success = False
                    fetch_errors.append("数据格式异常：缺少公告标题列")
            except Exception as e:
                print(f"❌ 数据处理异常: {e}")
                data_fetch_success = False
                fetch_errors.append(f"数据处理失败: {str(e)}")
        elif not data_fetch_success:
            print("❌ 数据获取阶段失败")
        
        # 4. 准备发送的消息
        print("\n📝 准备发送消息...")
        
        webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=dff99b4e-b4f4-44a5-87aa-9cb326de8777"
        final_message = ""
        
        if not data_fetch_success:
            # 数据获取失败
            error_details = "; ".join(fetch_errors)
            final_message = create_fallback_message(display_date_str, end_time, "network", error_details)
            
        elif not filtered_notices.empty:
            # 成功获取且有数据
            final_message = "# 🏛️ 司法拍卖公告提示 \n\n"
            final_message += f"**📊 统计时间：{display_date_str} 08:30 - {end_time.strftime('%Y年%m月%d日')} 08:30**\n\n"
            final_message += f"**📋 昨日司法拍卖提示信息共计 {len(filtered_notices)} 个，具体如下：**\n\n"
            
            final_message += "| 序号 | 股票代码 | 股票简称 | 公告标题 | 发布日期 |\n"
            final_message += "| :---: | :---: | :---: | :--- | :---: |\n"
            
            for i, (idx, row) in enumerate(filtered_notices.iterrows(), 1):
                stock_code_raw = str(row.get('代码', ''))
                stock_code_clean = stock_code_raw.split('.')[0]
                stock_code_fixed = stock_code_clean.zfill(6)
                
                stock_name = row.get('名称', '未知')
                title = row.get('公告标题', '无标题')[:50]  # 限制标题长度
                publish_date = row.get('公告日期', '未知日期')
                
                final_message += f"| {i} | {stock_code_fixed} | {stock_name} | {title} | {publish_date} |\n"
                
            final_message += f"\n**✅ 数据获取时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}**"
            
        else:
            # 成功获取但无数据
            final_message = f"# 🏛️ 司法拍卖公告提示 \n\n"
            final_message += f"**📊 统计时间：{display_date_str} 08:30 - {end_time.strftime('%Y年%m月%d日')} 08:30**\n\n"
            final_message += f"**📭 昨日无司法拍卖提示信息**\n\n"
            final_message += f"**✅ 数据获取时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}**"

        print("=" * 60)
        print("最终消息内容:")
        print(final_message)
        print("=" * 60)

        # 5. 发送消息
        print("\n📤 发送消息到企业微信...")
        send_success = send_wechat_message(final_message, webhook_url)
        
        if send_success:
            print("🎉 脚本执行成功完成！")
        else:
            print("⚠️ 脚本执行完成，但消息发送失败")
            
        return send_success
        
    except Exception as e:
        print(f"💥 脚本执行过程中发生未捕获的错误: {e}")
        traceback.print_exc()
        
        # 紧急错误通知
        error_message = f"# 🏛️ 司法拍卖公告提示 \n\n**💥 脚本执行崩溃**\n\n**错误信息：{str(e)[:200]}**\n\n**🚨 请立即联系管理员处理**"
        try:
            send_wechat_message(error_message, "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=dff99b4e-b4f4-44a5-87aa-9cb326de8777", max_retries=1)
        except:
            pass
            
        return False

if __name__ == "__main__":
    print("🚀 启动司法拍卖公告推送系统...")
    success = main()
    
    if success:
        print("✅ 系统正常退出")
        sys.exit(0)
    else:
        print("❌ 系统异常退出")
        sys.exit(1)