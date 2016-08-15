using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Configuration;
using System.Data;
using System.IO;
using System.Security.Cryptography;
using System.Net;
using Newtonsoft.Json;
using System.Text;


namespace Com.Baosight.Yu
{
    public class utils
    {
        /// <summary>
        /// 获取app参数
        /// </summary>
        /// <param name="key">读取appsetting 中的value</param>
        /// <returns></returns>
        public static string getappstr(string key)
        {
            string result = ConfigurationManager.AppSettings[key] == null ? "" : ConfigurationManager.AppSettings[key];
            return result;
        }

        /// <summary>
        /// datatable 转 csv
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="headers"></param>
        /// <param name="basepath"></param>
        /// <param name="baseurl"></param>
        /// <returns></returns>
        public static string[] dt2csv(DataTable dt, List<string> headers = null,string basepath="",string baseurl="")
        {
            string filename = Guid.NewGuid() + ".csv";//生成随机文件名
            string filepath = basepath + filename;//文件物理路径
            string urlstr = baseurl + filename;//文件发布路径、返回值
            try
            {
                using (FileStream tmpfile = File.Create(filepath))//创建文件
                {
                    using (StreamWriter sw = new StreamWriter(tmpfile,System.Text.Encoding.Default))//创建写入流
                    {
                        int colcount = dt.Columns.Count;//获取列数量
                        if (headers != null)//如果设置了表头，那么写入表头
                        {
                            sw.WriteLine(string.Join(",", headers));
                            sw.Flush();
                            tmpfile.Flush();
                        }

                        foreach (var data in dt.AsEnumerable())//读取数据
                        {
                            try
                            {
                                List<string> cells = new List<string>();
                                for (int i = 0; i < colcount; i++)
                                {
                                    cells.Add(data[i].ToString().Replace(",", "，"));//替换掉可能存在的逗号
                                }
                                sw.WriteLine(string.Join(",", cells));//写入数据
                                sw.Flush();
                                tmpfile.Flush();
                            }
                            catch (Exception ex)
                            {
                                continue;
                            }
                        }
                    }

                }
            }
            catch(Exception ex)
            {
                urlstr = "";
            }
            

            return new string[] { filepath,urlstr};
        }

        /// <summary>
        /// DES加解密
        /// </summary>
        /// <param name="sourcestr">原始字符串</param>
        /// <param name="flag">1 加密；2 解密</param>
        /// <returns></returns>
        public static string DESEncoding(string sourcestr,int flag,string key,string vi)
        {
            string result = string.Empty;

            byte[] bykey = System.Text.ASCIIEncoding.ASCII.GetBytes(key);
            byte[] byvi = System.Text.ASCIIEncoding.ASCII.GetBytes(vi);

            byte[] buffer = null;
            DESCryptoServiceProvider descsp = new DESCryptoServiceProvider();//des编码对象
            MemoryStream ms = null;
            CryptoStream cryStream = null;
            if (flag == 1)//加密
            {
                ms = new MemoryStream();//先创建 一个内存流
                cryStream = new CryptoStream(ms, descsp.CreateEncryptor(bykey,byvi), CryptoStreamMode.Write);//将内存流连接到加密转换流
                StreamWriter sw = new StreamWriter(cryStream);
                sw.WriteLine(sourcestr);//将要加密的字符串写入加密转换流
                sw.Close();
                cryStream.Close();
                buffer = ms.ToArray();//将加密后的流转换为字节数组
                result = Convert.ToBase64String(buffer);//将加密后的字节数组转换为字符串
            }
            else//解密
            {
                buffer = Convert.FromBase64String(sourcestr);
                ms = new MemoryStream(buffer);
                cryStream = new CryptoStream(ms, descsp.CreateDecryptor(bykey, byvi), CryptoStreamMode.Read);//内存流连接到解密流中
                StreamReader sr = new StreamReader(cryStream);
                result = sr.ReadLine();//将解密流读取为字符串
                sr.Close();
                cryStream.Close();
                ms.Close();
            }

            return result;
        }

        /// <summary>
        /// 使用Post方式调用REST服务
        /// </summary>
        /// <param name="resturl">服务地址URL</param>
        /// <param name="sourcedata">参数</param>
        /// <returns>返回值</returns>
        public static string postrestapi(string resturl,dynamic sourcedata)
        {
            var responseValue = string.Empty;//返回值
            HttpWebRequest req = WebRequest.Create(resturl) as HttpWebRequest;//httprequest对象
            req.Method = "POST";//post方式
            req.ContentType = "application/json";//json传递方式
            string s_data = JsonConvert.SerializeObject(sourcedata);//序列化对象
            
            //写入参数
            var bytes = Encoding.UTF8.GetBytes(s_data);
            req.ContentLength = bytes.Length;
            using (var writeStream = req.GetRequestStream())
            {
                writeStream.Write(bytes, 0, bytes.Length);
            }

            //发起请求并接收数据
            using (var response = (HttpWebResponse)req.GetResponse())
            {
                if (response.StatusCode != HttpStatusCode.OK)//调用失败返回空
                {
                    return string.Empty;
                }

                using (var responseStream = response.GetResponseStream())//获取返回的数据
                {
                    if (responseStream != null)
                        using (var reader = new StreamReader(responseStream))
                        {
                            responseValue = reader.ReadToEnd();
                        }
                }

            }
            return responseValue;
        }
        
    }
}