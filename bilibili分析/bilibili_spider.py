import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import time

# 生成近5年的季度日期列表（按照财报发布日期）
def generate_quarter_dates(years=5):
    dates = []
    current_year = datetime.now().year
    for year in range(current_year-years+1, current_year+1):
        for month in [3, 6, 9, 12]:
            date_str = f"{year}-{month:02d}-{30 if month in [6, 9] else 31}"
            dates.append(date_str)
    return dates

# 构建API请求URL
def build_url(report_date):
    base_url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
    params = {
        "reportName": "RPT_USF10_INFO_PRODUCTSTRUCTURE",
        "columns": "SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,ORG_CODE,REPORT_DATE,CURRENCY,PRODUCT_NAME,MAIN_BUSINESS_INCOME,MBI_RATIO,IS_TOTAL",
        "filter": f"(SECUCODE=\"BILI.O\")(REPORT_DATE='{report_date}')(IS_TOTAL=\"0\")",
        "pageNumber": 1,
        "pageSize": 200,
        "source": "SECURITIES",
        "client": "PC",
        "v": str(time.time()).replace(".", "")[:16]
    }
    
    query_string = "&".join([f"{k}={v}" for k, v in params.items()])
    return f"{base_url}?{query_string}"

# 计算单季度数据
def calculate_quarterly_data(df):
    # 确保数据已按日期排序
    df = df.sort_values(by=["REPORT_DATE"])
    
    # 添加年份和季度列
    df["YEAR"] = pd.to_datetime(df["REPORT_DATE"]).dt.year
    df["MONTH"] = pd.to_datetime(df["REPORT_DATE"]).dt.month
    df["QUARTER"] = df["MONTH"].map({3: 1, 6: 2, 9: 3, 12: 4})
    
    # 创建一个新的DataFrame存储原始累计值
    df_cumulative = df.copy()
    
    # 创建一个新的DataFrame用于存储单季度值
    df_quarterly = pd.DataFrame()
    
    # 按产品名称分组
    for product, group in df.groupby("PRODUCT_NAME"):
        # 按年份排序
        group = group.sort_values(by=["YEAR", "QUARTER"])
        
        # 初始化单季度收入列
        group["QUARTERLY_INCOME"] = group["MAIN_BUSINESS_INCOME"]
        
        # 计算单季度值
        for i in range(len(group)-1):
            current_row = group.iloc[i]
            next_row = group.iloc[i+1]
            
            # 如果是同一年的数据，且当前是第一季度以上
            if (current_row["YEAR"] == next_row["YEAR"] and 
                current_row["QUARTER"] < next_row["QUARTER"]):
                # 下一季度的单季度值 = 下一季度的累计值 - 当前季度的累计值
                idx = next_row.name
                group.at[idx, "QUARTERLY_INCOME"] = (
                    next_row["MAIN_BUSINESS_INCOME"] - current_row["MAIN_BUSINESS_INCOME"]
                )
        
        # 将处理后的数据添加到结果DataFrame
        df_quarterly = pd.concat([df_quarterly, group])
    
    # 重置索引
    df_quarterly = df_quarterly.reset_index(drop=True)
    
    # 计算单季度收入占比
    total_quarterly = df_quarterly.groupby(["YEAR", "QUARTER"])["QUARTERLY_INCOME"].sum().reset_index()
    total_quarterly = total_quarterly.rename(columns={"QUARTERLY_INCOME": "TOTAL_QUARTERLY_INCOME"})
    
    # 合并总收入数据
    df_quarterly = pd.merge(
        df_quarterly, 
        total_quarterly, 
        on=["YEAR", "QUARTER"], 
        how="left"
    )
    
    # 计算单季度收入占比
    df_quarterly["QUARTERLY_RATIO"] = df_quarterly["QUARTERLY_INCOME"] / df_quarterly["TOTAL_QUARTERLY_INCOME"]
    
    return df_cumulative, df_quarterly

# 主函数
def main():
    # 生成近5年的季度日期
    quarter_dates = generate_quarter_dates(5)
    
    # 用于存储所有数据的列表
    all_data = []
    
    # 遍历每个季度日期爬取数据
    for date in quarter_dates:
        try:
            url = build_url(date)
            print(f"正在爬取 {date} 的数据...")
            
            response = requests.get(url)
            if response.status_code == 200:
                json_data = response.json()
                
                # 检查是否成功获取数据
                if json_data.get("success") and json_data.get("result", {}).get("data"):
                    data_list = json_data["result"]["data"]
                    
                    # 将日期信息添加到每条记录中
                    for item in data_list:
                        # 截取日期部分
                        if "REPORT_DATE" in item and item["REPORT_DATE"]:
                            item["REPORT_DATE"] = item["REPORT_DATE"].split()[0]
                    all_data.extend(data_list)
                        
                    print(f"成功获取 {date} 的数据，共 {len(data_list)} 条记录")
                else:
                    print(f"未获取到 {date} 的数据")
            else:
                print(f"请求失败，状态码：{response.status_code}")
                
            # 添加延时，避免请求过于频繁
            time.sleep(1)
        except Exception as e:
            print(f"处理 {date} 数据时出错: {str(e)}")
    
    # 如果成功获取了数据，将其转换为DataFrame并保存为CSV
    if all_data:
        df = pd.DataFrame(all_data)
        
        # 将数值列转换为数值类型
        numeric_columns = ["MAIN_BUSINESS_INCOME", "MBI_RATIO"]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # 按日期和产品名称排序
        df = df.sort_values(by=["REPORT_DATE", "PRODUCT_NAME"])
        
        # 保存原始累计值数据
        csv_filename_cumulative = "bilibili_financial_data_cumulative.csv"
        df.to_csv(csv_filename_cumulative, index=False, encoding="utf-8-sig")
        print(f"累计值数据已保存至 {csv_filename_cumulative}，共 {len(df)} 条记录")
        
        # 计算单季度数据
        df_cumulative, df_quarterly = calculate_quarterly_data(df)
        
        # 保存单季度数据
        csv_filename_quarterly = "bilibili_financial_data_quarterly.csv"
        df_quarterly.to_csv(csv_filename_quarterly, index=False, encoding="utf-8-sig")
        print(f"单季度数据已保存至 {csv_filename_quarterly}，共 {len(df_quarterly)} 条记录")
        
        # 生成分析报告
        generate_analysis_report(df_quarterly)
    else:
        print("未获取到任何数据")

# 生成简单的分析报告
def generate_analysis_report(df):
    # 按年度和季度分组，计算各业务线的收入及占比
    df_pivot = df.pivot_table(
        index=["YEAR", "QUARTER"], 
        columns="PRODUCT_NAME", 
        values="QUARTERLY_INCOME",
        aggfunc="sum"
    ).reset_index()
    
    # 计算总收入
    df_pivot["总收入"] = df_pivot.iloc[:, 2:].sum(axis=1)
    
    # 保存分析报告
    report_filename = "bilibili_quarterly_analysis.csv"
    df_pivot.to_csv(report_filename, index=False, encoding="utf-8-sig")
    print(f"季度分析报告已保存至 {report_filename}")

if __name__ == "__main__":
    main()