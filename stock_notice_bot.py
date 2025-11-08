# -*- coding: utf-8 -*-
"""
A股司法拍卖公告自动推送机器人
功能：每日自动获取司法拍卖公告并推送到企业微信
版本：v1.3 - 添加测试模式，优化重复推送检查
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta, timezone
import requests
import json
import sys
import traceback
import time
import os


class StockNoticeBot:
    """司法拍卖公告推送机器人"""
    
    def __init__(self):
        self.webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=dff99b4e-b4f4-44a5-87aa-9cb326de8777"
        self.beijing_tz = timezone(timedelta(hours=8))  # 北京时区 UTC+8
        self.is_test_mode = os.getenv('GITHUB_ACTIONS') is None  # 本地运行时为测试模式
        
    def get_beijing_time(self):
        """获取当前北京时间"""
        return datetime.now(self.beijing_tz)
    
    def format_beijing_time(self, dt=None):
        """格式化时间为北京时间字符串"""
        if dt is None:
            dt = self.get_beijing_time()
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    
    def should_send_message(self):
        """
        检查是否应该发送消息
        返回: (should_send, reason)
        """
        current_time = self.get_beijing_time()
        current_date = current_time.date()
        
        # 测试模式：总是发送
        if self.is_test_mode:
            return True, "测试模式：允许发送"
        
        # 生产环境：检查是否在同一天内已经发送过
        # 这里使用简单的时间判断，实际部署中可以更复杂
        if current_time.hour < 12:  # 只在中午前执行
            return True, "生产环境：允许发送"
        else:
            return False, "生产环境：今日已过发送时段"
    
    def validate_config(self):
        """验证配置"""
        if not self.webhook_url.startswith('https://qyapi.weixin.qq.com/cgi-bin/webhook/send'):
            return False, "企业微信Webhook地址格式不正确"
        return True, "配置验证通过"
    
    def send_wechat_message(self, content, max_retries=3):
        """发送消息到企业微信机器人"""
        data = {
            "msgtype": "markdown_v2",
            "markdown_v2": {"content": content}
        }
        
        headers = {"Content-Type": "application/json"}
        
        for attempt in range(max_retries):
            try:
                print(f"第{attempt+1}次尝试发送消息...")
                response = requests.post(
                    self.webhook_url, 
                    data=json.dumps(data), 
                    headers=headers, 
                    timeout=15
                )
                
                if response.status_code == 200:
                    result = response.json()
                    if result.get('errcode') == 0:
                        print("✅ 消息已成功发送到企业微信群！")
                        return True
                    else:
                        print(f"❌ 企业微信接口返回错误: {result}")
                else:
                    print(f"❌ 网络请求失败，状态码: {response.status_code}")
                    
            except Exception as e:
                print(f"❌ 第{attempt+1}次发送异常: {e}")
            
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 10
                print(f"🔄 等待{wait_time}秒后重试发送...")
                time.sleep(wait_time)
        
        print("❌ 消息发送失败，已超过最大重试次数")
        return False
    
    def get_notice_data(self, date_str, max_retries=3):
        """获取公告数据"""
        for attempt in range(max_retries):
            try:
                print(f"📡 第{attempt+1}次尝试获取 {date_str} 的公告...")
                start_time = time.time()
                
                day_df = ak.stock_notice_report(date=date_str)
                elapsed_time = time.time() - start_time
                
                print(f"✅ 请求成功，耗时: {elapsed_time:.2f}秒")
                
                # 验证数据格式
                if day_df is None:
                    print("❌ 接口返回None")
                    continue
                    
                if not isinstance(day_df, pd.DataFrame):
                    print(f"❌ 返回数据类型错误: {type(day_df)}")
                    continue
                    
                if day_df.empty:
                    print("ℹ️ 该日无公告数据")
                    return day_df
                
                # 检查必要列
                required_columns = ['公告标题', '代码', '名称', '公告日期']
                missing_columns = [col for col in required_columns if col not in day_df.columns]
                if missing_columns:
                    print(f"⚠️ 数据缺少必要列: {missing_columns}")
                
                print(f"📊 获取到 {len(day_df)} 条公告")
                return day_df
                
            except Exception as e:
                error_msg = str(e)
                print(f"❌ 第{attempt+1}次获取失败: {error_msg}")
                
                # 错误分类提示
                if any(keyword in error_msg for keyword in ['Connection', 'proxy', 'timeout', 'SSL']):
                    print("🌐 网络连接问题")
                else:
                    print("❓ 未知错误")
            
            # 指数退避重试
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) * 5
                print(f"🔄 等待{wait_time}秒后重试... (剩余重试次数: {max_retries - attempt - 1})")
                time.sleep(wait_time)
        
        print(f"❌ 获取 {date_str} 数据失败，已超过最大重试次数")
        return None
    
    def filter_auction_notices(self, notices_df):
        """筛选司法拍卖公告"""
        if notices_df.empty:
            return pd.DataFrame()
            
        if '公告标题' not in notices_df.columns:
            print("⚠️ 数据中缺少'公告标题'列")
            return pd.DataFrame()
        
        filtered = notices_df[
            notices_df['公告标题'].str.contains('拍卖', na=False) & 
            notices_df['公告标题'].str.contains('提示性', na=False)
        ]
        print(f"🎯 筛选出司法拍卖公告: {len(filtered)} 条")
        return filtered
    
    def create_message(self, display_date_str, end_time, data_status, filtered_notices=None, error_details=""):
        """创建推送消息"""
        # 修改时间范围显示为6:00
        base_message = f"# 🏛️ 司法拍卖公告提示 \n\n**📊 统计时间：{display_date_str} 06:00 - {end_time.strftime('%Y年%m月%d日')} 06:00**\n\n"
        current_time = self.format_beijing_time()
        
        # 添加模式标识
        mode_indicator = " (测试模式)" if self.is_test_mode else ""
        
        if data_status == "success_with_data":
            message = base_message
            message += f"**📋 昨日司法拍卖提示信息共计 {len(filtered_notices)} 个，具体如下：**\n\n"
            message += "| 序号 | 股票代码 | 股票简称 | 公告标题 | 发布日期 |\n"
            message += "| :---: | :---: | :---: | :--- | :---: |\n"
            
            for i, (_, row) in enumerate(filtered_notices.iterrows(), 1):
                stock_code = str(row.get('代码', '')).split('.')[0].zfill(6)
                stock_name = row.get('名称', '未知')
                title = row.get('公告标题', '无标题')[:50]  # 限制标题长度
                publish_date = row.get('公告日期', '未知日期')
                
                message += f"| {i} | {stock_code} | {stock_name} | {title} | {publish_date} |\n"
                
            message += f"\n**✅ 数据获取时间：{current_time} (北京时间){mode_indicator}**"
            
        elif data_status == "success_no_data":
            message = base_message
            message += f"**📭 昨日无司法拍卖提示信息**\n\n"
            message += f"**✅ 数据获取时间：{current_time} (北京时间){mode_indicator}**"
            
        else:  # data_status == "failed"
            message = base_message
            message += f"**❌ 数据获取失败**\n\n**错误详情：{error_details}**\n\n"
            message += f"**💡 状态：已自动重试多次，明天将再次尝试**\n\n"
            message += f"**✅ 最后尝试时间：{current_time} (北京时间){mode_indicator}**"
        
        return message
    
    def run(self):
        """主运行逻辑"""
        script_start_time = time.time()
        max_script_runtime = 600  # 10分钟最大运行时间
        
        try:
            print("=" * 60)
            print("🏁 开始执行A股司法拍卖公告查询...")
            if self.is_test_mode:
                print("🔬 当前运行在测试模式")
            else:
                print("🚀 当前运行在生产模式")
            print("=" * 60)
            
            # 重复推送检查
            should_send, reason = self.should_send_message()
            if not should_send:
                print(f"⏸️ {reason}，脚本终止执行")
                return True
            
            # 配置验证
            print("🔧 验证配置...")
            config_valid, config_msg = self.validate_config()
            if not config_valid:
                error_message = self.create_message("", self.get_beijing_time(), "failed", error_details=config_msg)
                self.send_wechat_message(error_message)
                return False
            
            print("✅ 配置验证通过")
            
            # 计算时间范围（使用北京时间，调整为6:00）
            now = self.get_beijing_time()
            end_time = now.replace(hour=6, minute=0, second=0, microsecond=0)  # 修改为6:00
            start_time = end_time - timedelta(days=1)

            display_date_str = start_time.strftime('%Y年%m月%d日')
            
            print(f"📅 查询时间范围: {start_time} 至 {end_time}")
            print(f"📅 查询日期: {start_time.strftime('%Y%m%d')} 到 {end_time.strftime('%Y%m%d')}")

            # 获取公告数据
            print("\n📡 开始获取公告数据...")
            
            df_list = []
            data_fetch_success = True
            fetch_errors = []
            
            dates_to_fetch = [
                start_time.strftime('%Y%m%d'),
                (start_time + timedelta(days=1)).strftime('%Y%m%d')
            ]
            
            for date_str in dates_to_fetch:
                # 检查脚本运行时间
                if time.time() - script_start_time > max_script_runtime:
                    print("⏰ 脚本运行时间过长，提前结束")
                    break
                    
                day_df = self.get_notice_data(date_str)
                
                if day_df is None:
                    data_fetch_success = False
                    fetch_errors.append(f"日期 {date_str} 获取失败")
                elif not day_df.empty:
                    df_list.append(day_df)
                    print(f"✅ 日期 {date_str} 处理完成")
                else:
                    print(f"ℹ️ 日期 {date_str} 无数据")
            
            # 数据处理和筛选
            filtered_notices = pd.DataFrame()
            
            if data_fetch_success and df_list:
                try:
                    all_notices_df = pd.concat(df_list, ignore_index=True)
                    print(f"📊 合并后总公告数: {len(all_notices_df)} 条")
                    filtered_notices = self.filter_auction_notices(all_notices_df)
                    
                except Exception as e:
                    print(f"❌ 数据处理异常: {e}")
                    data_fetch_success = False
                    fetch_errors.append(f"数据处理失败: {str(e)}")
            
            # 准备发送的消息
            print("\n📝 准备发送消息...")
            
            if not data_fetch_success:
                error_details = "; ".join(fetch_errors)
                final_message = self.create_message(display_date_str, end_time, "failed", error_details=error_details)
            elif not filtered_notices.empty:
                final_message = self.create_message(display_date_str, end_time, "success_with_data", filtered_notices)
            else:
                final_message = self.create_message(display_date_str, end_time, "success_no_data")

            print("=" * 60)
            print("最终消息内容:")
            print(final_message)
            print("=" * 60)

            # 发送消息
            print("\n📤 发送消息到企业微信...")
            send_success = self.send_wechat_message(final_message)
            
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
                self.send_wechat_message(error_message, max_retries=1)
            except:
                pass
                
            return False


def main():
    """主函数"""
    print("🚀 启动司法拍卖公告推送系统...")
    bot = StockNoticeBot()
    success = bot.run()
    
    if success:
        print("✅ 系统正常退出")
        sys.exit(0)
    else:
        print("❌ 系统异常退出")
        sys.exit(1)


if __name__ == "__main__":
    main()
