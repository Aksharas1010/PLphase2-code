using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Aspose.Zip.Saving;
using Aspose.Zip;
using GeneratePnLReport.DTO;
using GeneratePnLReport.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Net.Mail;
using ClosedXML.Excel;
using GeneratePnLReport.Utils;
using System.Data.SqlClient;
using DocumentFormat.OpenXml;
using System.Data;
using iTextSharp.text.pdf;
using iTextSharp.text;
using System.Net.Sockets;
using System.Net;
using System.Text;
using DocumentFormat.OpenXml.Office.CustomUI;
using DocumentFormat.OpenXml.Spreadsheet;
using System.Text.RegularExpressions;

namespace GeneratePnLReport
{
    internal class Worker : BackgroundService
    {
        private static readonly object _object = new object();

        private readonly ILogger<Worker> _logger;
        //private static readonly ILog _logger = LogManager.GetLogger(typeof(Worker));
        public Worker(ILogger<Worker> logger) => (_logger) = (logger);
        PnLInputModel pnLInputModel_for_log = null;

        static int queryTimeout = !string.IsNullOrEmpty(ConfigurationManager.AppSettings["queryTimeout"]) ?
        Convert.ToInt32(ConfigurationManager.AppSettings["queryTimeout"]) : 1800;

        private int _maxRetryCount = Convert.ToInt32(ConfigurationManager.AppSettings["ProcessRetryCount"]);

        void Run()
        {
            List<string> files = new List<string>();

            try
            {
                using (GCCEntities gcc = new GCCEntities())
                {
                    gcc.Database.CommandTimeout = queryTimeout;
                    var closingRateForLongTerCapGainasJanuary = gcc.ClosingRateForLongTerCapGainasJanuaries.FirstOrDefault();
                    var pnLInputModels = gcc.Database.SqlQuery<PnLInputModel>($"exec SpGetTaxPendingRequest_V1 {_maxRetryCount}").ToList();

                    pnLInputModels = pnLInputModels.Where(p => !string.IsNullOrEmpty(p.ReportType) && p.ReportType.Trim().ToLower() == "clear tax format").ToList();

                    foreach (var item in pnLInputModels)
                    {
                        pnLInputModel_for_log = item;

                        if (item.ClientId <= 0)
                        {
                            _logger.LogInformation($"Invalid client id: {item.ClientId}");
                            return;
                        }

                        var query = $"EXEC SpTax_GeneratePandL_V5 {item.RefId},{item.ClientId},'{item.FiscYear}','','N','N'";
                       // var query = $"EXEC SpTax_GeneratePandL_V4 {item.RefId},{item.ClientId},'{item.FiscYear}','','N','N'";
                        
                        _logger.LogInformation($"Starts for RefId: {item.RefId} on: {DateTime.Now}\n Query: {query}");

                        _logger.LogInformation($"{query} started at {DateTime.Now}.");
                        
                        var equityResult = gcc.Database.SqlQuery<PnLOutputModel>(query).ToList();

                        _logger.LogInformation($"{query} ended at {DateTime.Now}.");

                        var start = item.FiscYear.Split('-')[0] + "-04-01";
                        var end = item.FiscYear.Split('-')[1] + "-03-31";

                        var client = gcc.CLIENTs.FirstOrDefault(p => p.CLIENTID == item.ClientId);

                        var fileName = GeneratePnLExcel("Profit & Loss_" + item.ClientId + "_" + item.FiscYear + DateTime.Now.ToString("yyyyMMddhhmmssfff"),
                            client, start, end, closingRateForLongTerCapGainasJanuary, item);

                        files.Add(fileName);

                        fileName = GenerateBuyNotFoundExcel(item.RefId, item.ClientId, client, start, end);
                        
                        if(!string.IsNullOrEmpty(fileName))
                            files.Add(fileName);

                        var fileNames = GenerateSTTFormNo10dbAutoTaxStatement(client, item.FiscYear);
                        if (fileNames.Any())
                            files.AddRange(fileNames);

                        var clientsforTaxComputationQuery = $"exec SpGetClientsforTaxComputation '{client.CURLOCATION.Trim() + client.TRADECODE.Trim()}', '{item.RefId}'";
                        //var clientsforTaxComputationQuery = $"exec SpGetClientsforTaxComputation 'THR001', '{item.RefId}'";
                        var computationRefernceModel = gcc.Database.SqlQuery<TaxComputationReferneceModel>(clientsforTaxComputationQuery).ToList().FirstOrDefault();
                        if (computationRefernceModel != null)
                        {
                            fileName = MakeZip(files, computationRefernceModel.PDFPassword, client);
                            files.Add(fileName);
                            var emailSubject = $"Tax Computation Statement ({item.FiscYear}) {client.CURLOCATION.Trim() + client.TRADECODE.Trim()}";
                            var isSuccess = SendEmailEmailToClientWithAttachment(fileName, computationRefernceModel.ClientEmail, computationRefernceModel.LocEmail, computationRefernceModel.Name, emailSubject);

                            if (isSuccess)
                            {
                                gcc.Database.SqlQuery<int>($"SpUpdateTaxPendingRequest {item.RefId}, 'Y'").ToList();

                                var emailLogQuery = "SPTaxStatementEmailLog @RefId, @Tradecode, @Financialyear, @SenderID, @RecipientID, @CC, @BCC, @Subject, @Attachmentname, @IP, @Euser";

                                gcc.Database.ExecuteSqlCommand(emailLogQuery,
                                    new SqlParameter("RefId", item.RefId), 
                                    new SqlParameter("Tradecode", client.CURLOCATION.Trim() + client.TRADECODE.Trim()),
                                    new SqlParameter("Financialyear", item.FiscYear), 
                                    new SqlParameter("SenderID", ConfigurationManager.AppSettings["FromEmail"]),
                                    new SqlParameter("RecipientID", computationRefernceModel.ClientEmail),
                                    new SqlParameter("CC", computationRefernceModel.LocEmail), 
                                    new SqlParameter("BCC", " "),
                                    new SqlParameter("Subject", emailSubject), 
                                    new SqlParameter("Attachmentname", fileName),
                                    new SqlParameter("IP", GetLocalIPAddress()), 
                                    new SqlParameter("Euser", " ")
                                    );
                            }
                        }
                        else
                        {
                            _logger.LogInformation($"Result from SpGetClientsforTaxComputation is null, deleting generated files for client: {item.ClientId} and refId: {item.RefId}");
                        }
                        _logger.LogInformation($"Ends for RefId: {item.RefId} on: {DateTime.Now}\n Query: {query}");
                    }
                }
            }
            catch (Exception ex)            
            {
                if (pnLInputModel_for_log != null)
                {
                    var subject = $"Error while generating P&L for the refId: {pnLInputModel_for_log.RefId}, client: {pnLInputModel_for_log.ClientId}, Year: {pnLInputModel_for_log.FiscYear}";
                    var message = $"Error: {ex}";
                    var fromEmail = ConfigurationManager.AppSettings["ErrorDetailFromEmail"];
                    var toEmail = ConfigurationManager.AppSettings["ErrorDetailToEmail"];

                    SendErrorOrStopRequestEmail(subject, message, fromEmail, toEmail);

                    _logger.LogError($"{subject} \nError occured on {DateTime.Now}: {ex}");                    

                    UpdateProcessRetryCount(
                        pnLInputModel_for_log.RefId, 
                        pnLInputModel_for_log.ClientId, 
                        pnLInputModel_for_log.RetryCount ?? 0,
                        fromEmail,
                        toEmail,
                        pnLInputModel_for_log.FiscYear);
                }
            }
            finally
            {
                files.ForEach(p => File.Delete(ConfigurationManager.AppSettings["FileSavePath"] + p));
            }
        }

        private void UpdateProcessRetryCount(
            int refId, 
            int clientId, 
            int retryCount, 
            string fromEmail, 
            string toEmail,
            string fiscYear)
        {
            using (GCCEntities gcc = new GCCEntities())
            {
                gcc.Database.ExecuteSqlCommand($"exec spUpdatePendingTaxRequestRetryCount {refId},{clientId}");
            }

            if ((retryCount + 1) == _maxRetryCount)
            {
                SendErrorOrStopRequestEmail(
                    $"P&L generation skipped for the refId: {refId}",
                    $"P&L generation for the refId: {refId}, client: {clientId}, Year: {fiscYear} reached its maximum retry attempt(s) of {_maxRetryCount}. The request will be skipped and the next consecutive request wil be processed.",
                    fromEmail,
                    toEmail);
                return;
            }

            _logger.LogInformation($"P&L generation for the refId: {refId} will be retried.");
        }

        private string GeneratePnLExcel(string fileName, CLIENT client, string start, string end, ClosingRateForLongTerCapGainasJanuary closingRateForLongTerCapGainasJanuary, PnLInputModel input)
        {
            using (var wbook = new XLWorkbook())
            {
                var summaryLastRow = 6;
                var summaryColumnIndex = 0;
                MyResult[] EQLongresultArray=new MyResult[0];
                MyResult[] EQShortresultArray= new MyResult[0];
                MyResult[] EQforesultArray = new MyResult[0];
                MyResult[] EQcdsresultArray = new MyResult[0];
                MyResult[] EQcmresultArray = new MyResult[0];
                MyResult[] EQboresultArray = new MyResult[0];
                var xlSummaryWorkSheet = wbook.Worksheets.Add("Summary");
                {
                    ExcelUtils.InsertPicture(xlSummaryWorkSheet, Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location) + "\\" + ConfigurationManager.AppSettings["ImagePath"], 1, 1);

                    ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex + 2, "Trade Code");
                    ExcelUtils.FormatCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex + 2, false);
                    ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex + 3, client.CURLOCATION.Trim() + client.TRADECODE.Trim());
                    ExcelUtils.FormatCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex + 3, false);

                    ExcelUtils.SetValueToCell(xlSummaryWorkSheet, ++summaryLastRow, summaryColumnIndex + 2, "Client Name");
                    ExcelUtils.FormatCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex + 2, false);
                    ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex + 3, client.NAME);
                    ExcelUtils.FormatCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex + 3, false);

                    ExcelUtils.SetValueToCell(xlSummaryWorkSheet, ++summaryLastRow, summaryColumnIndex + 2, "PAN");
                    ExcelUtils.FormatCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex + 2, false);
                    ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex + 3, client.PAN_GIR);
                    ExcelUtils.FormatCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex + 3, false);

                    summaryLastRow = 11;
                    ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex + 2, "Summary of Profit & Loss for the FY " + Convert.ToDateTime(start).Year +
                        "to" + Convert.ToDateTime(end).Year);
                    ExcelUtils.FormatCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex + 2, false);

                    string[] summaryEquityHeadingsArray = { "Segment", "Type", "Buy Value", "Sell Value", "Expense", "Taxable Profit/ Loss" };

                    for (int k = 0; k < summaryEquityHeadingsArray.Length; k++)
                    {
                        ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, k + 2, summaryEquityHeadingsArray[k]);
                        ExcelUtils.FormatCell(xlSummaryWorkSheet, summaryLastRow, k + 2, true);
                    }
                    summaryLastRow++;
                }

                try
                {
                    //throw new Exception("Error");

                    int i = 0;
                    int j = 0;
                    var lastRow = 6;
                    var mgopenvalue = "";
                    var mgcloseval = "";
                    List<Tax_Profit_Details_Cash> cleartaxResult = new List<Tax_Profit_Details_Cash>();
                    #region Equity
                    var xlEquityWorkSheet = wbook.Worksheets.Add("Equity");
                    {
                        using (GCCEntities gcc = new GCCEntities())
                        {
                            gcc.Database.CommandTimeout = queryTimeout;

                            var dataheadings = "Symbol,Description,ISIN,Entry Date (Buy),Exit Date (Sell),Quantity,Buy Value,Sell Value,Buy & Sell Expense ,Net Profit/Loss ,Period of Holding (Days),Taxable Profit, Turnover";
                            var dataLongTermheadings = "Symbol,Description,ISIN,Entry Date (Buy),Exit Date (Sell),Quantity,Buy Value,Sell Value," +
                                "Buy & Sell Expense ,Net Profit/Loss ,Period of Holding (Days),Fair Market Rate(31.01.2018), Fair Market Value(31.01.2018),Taxable Profit/Loss, Turnover"; var taxHeadings = "Buy Brokerage,Buy GST,Buy Exchange Levy,Buy Stamp Duty,Total Buy Cost,Sale Bokerage,Sale GST,Sale Exchange Levy,Sale Stamp Duty,Total Sell Cost";
                            var equitySummayHeadings = "Segment,Type,Buy Value,Sell Value,Expense,Net Profit/ Loss,Taxable Profit/Loss,Turnover";
                            string[] equitySummayHeadingsArray = equitySummayHeadings.Split(',');

                            // var equityResult = 

                           
                              var equityResult = gcc.Database.SqlQuery<Tax_Profit_Details_Cash>($"SpTaxPandLExcel_V2 {input.RefId},{input.ClientId},'{input.FiscYear}','Equity'").ToList();
                            //gcc.Tax_Profit_Details_Cash.Where(p => p.Clientid == clientId && p.RefId == refId).ToList();
                            cleartaxResult = equityResult;
                            var groupedData = equityResult.GroupBy(p => p.Type);

                            ExcelUtils.InsertPicture(xlEquityWorkSheet, System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location) + "\\" + ConfigurationManager.AppSettings["ImagePath"], 1, 1);


                            ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i + 2, "Trade Code");
                            ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i + 3, client.CURLOCATION.Trim() + client.TRADECODE.Trim());
                            ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlEquityWorkSheet, ++lastRow, i + 2, "Client Name");
                            ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i + 3, client.NAME);
                            ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlEquityWorkSheet, ++lastRow, i + 2, "PAN");
                            ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i + 3, client.PAN_GIR);
                            ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlEquityWorkSheet, ++lastRow, i + 2, "Segment");
                            ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i + 3, "Equity");
                            ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i + 3, false);

                            lastRow = 11;
                            for (i = 0; i < equitySummayHeadingsArray.Length; i++)
                            {
                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i + 2, equitySummayHeadingsArray[i]);
                                ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i + 2, true);
                            }

                            lastRow += 1;
                            i = 1;
                            var taxableProfitSummaryRow = 0;
                            var taxableProfitSummarycolumn = 0;

                            var turnOverSpeculationSummaryRow = 0;
                            var turnOverSpeculationSummaryColumn = 0;

                            var turnOverShortTermSummaryRow = 0;
                            var turnOverShortTermSummaryColumn = 0;
                            
                            var turnOverBuyBackSummaryRow = 0;
                            var turnOverBuyBackSummaryColumn = 0;

                            foreach (var group in groupedData)
                            {
                                var totalExpense = group.Sum(p => p.PurchaseBrokerage + p.PurchaseExchangeLevy + p.PurchaseServiceTax + p.PurchaseStampDuty
                                                                      + p.SaleBrokerage + p.SaleExchangeLevy + p.SaleServiceTax + p.SaleStampDuty);
                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, "Equity");
                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, group.Key);
                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, group.Sum(p => p.BuyValue));
                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, group.Sum(p => p.SaleValue));
                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, totalExpense);
                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, group.Sum(p => p.SaleValue) - group.Sum(p => p.BuyValue) - totalExpense);
                                if (group.Key.ToLower() != "buy back") { 
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, group.Sum(p => p.SaleValue) - group.Sum(p => p.BuyValue) - totalExpense);
                                }
                                if (group.Key.ToLower() == "long term")
                                {
                                    taxableProfitSummaryRow = lastRow;
                                    taxableProfitSummarycolumn = i;
                                }
                                else if (group.Key.ToLower() == "speculation")
                                {
                                    turnOverSpeculationSummaryColumn = i;
                                    turnOverSpeculationSummaryRow = lastRow;
                                }
                                else if (group.Key.ToLower() == "short term")
                                {
                                    turnOverShortTermSummaryColumn = i;
                                    turnOverShortTermSummaryRow = lastRow;
                                }
                                else if (group.Key.ToLower() == "buy back")
                                {
                                    turnOverBuyBackSummaryColumn = i;
                                    turnOverBuyBackSummaryRow = lastRow;
                                }
                                lastRow++;
                                i = 1;
                            }

                            i = 0;
                            lastRow = 17;
                            ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i + 2, "Total STT Paid");
                            ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i + 3, "Please refer attached 10DB form for details.");

                            lastRow = 19;
                            ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i + 2, "Tax Transaction Details from " + Convert.ToDateTime(start).ToString("dd-MM-yyyy") + " to " + Convert.ToDateTime(end).ToString("dd-MM-yyyy"));
                            ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i + 2, false);

                            lastRow = 21;

                            GCCEntities gCCEntities = new GCCEntities();
                            var closingRateForLongTerCapGainasJanuaryList = gCCEntities.ClosingRateForLongTerCapGainasJanuaries.ToList();
                            foreach (var groupedItem in groupedData)
                            {

                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i + 2, groupedItem.Key.ToLower() == "speculation" ? "Equity (Intraday/Speculative)" : "Equity(" + groupedItem.Key + ")");
                                ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i + 2, false);
                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, 18, "Expense Breakup");
                                ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, 18, false);
                                lastRow++;
                                //var speculationItems = tax_Profit_Details_Cashes.Where(p => p.Type.ToLower() == "speculation");

                                string[] dataheadingsArray = dataheadings.Split(',');
                                string[] taxHeadingsArray = taxHeadings.Split(',');
                                if (groupedItem.Key.ToLower() != "long term")
                                {
                                    if (groupedItem.Key.ToLower() == "buy back")
                                    {
                                        for (i = 0, j = 18; i < dataheadingsArray.Length-2; i++)
                                        {
                                            ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i + 2, dataheadingsArray[i]);
                                            ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i + 2, true);
                                        }
                                    }
                                    else
                                    {
                                        for (i = 0, j = 18; i < dataheadingsArray.Length; i++)
                                        {
                                            ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i + 2, dataheadingsArray[i]);
                                            ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i + 2, true);
                                        }
                                    }
                                        
                                }
                                else
                                {
                                    string[] dataLongTermheadingsArray = dataLongTermheadings.Split(',');
                                    for (i = 0, j = 18; i < dataLongTermheadingsArray.Length; i++)
                                    {
                                        ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i + 2, dataLongTermheadingsArray[i]);
                                        ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i + 2, true);
                                    }
                                }

                                for (i = 0, j = 18; i < taxHeadingsArray.Length; i++)
                                {
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, j++, taxHeadingsArray[i]);
                                    ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, j - 1, true);
                                }

                                lastRow++;
                                i = 1;
                                j = 18;
                                decimal taxableProfit = 0;

                                foreach (var item in groupedItem)
                                {
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, item.Security);
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, item.Description);
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, item.ISIN);
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, item.TranDateBuy);
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, item.TranDateSale);
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, item.SaleQty);
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, item.BuyValue);
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, item.SaleValue);
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, item.PurchaseBrokerage + item.PurchaseExchangeLevy
                                                                      + item.PurchaseServiceTax + item.PurchaseStampDuty
                                                                      + item.SaleBrokerage + item.SaleExchangeLevy +
                                                                      item.SaleServiceTax + item.SaleStampDuty);

                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, item.SaleValue - (item.BuyValue + item.PurchaseBrokerage + item.PurchaseExchangeLevy
                                                                      + item.PurchaseServiceTax + item.PurchaseStampDuty
                                                                      + item.SaleBrokerage + item.SaleExchangeLevy +
                                                                      item.SaleServiceTax + item.SaleStampDuty));
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, item.DayToSell.HasValue && item.DayToSell > 0 ? item.DayToSell : 0);
                                    decimal? fairMarketBuyValue = null;
                                    if (groupedItem.Key.ToLower() == "long term")
                                    {

                                        if (item.TranDateBuy.GetValueOrDefault().Date <= new DateTime(2018, 1, 31).Date
                                            && item.TranDateSale.GetValueOrDefault().Date >= new DateTime(2018, 2, 1).Date)
                                        {
                                            ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, closingRateForLongTerCapGainasJanuaryList.Any(p => p.Security.Trim().ToLower() == item.Security.Trim().ToLower()) ?
                                            closingRateForLongTerCapGainasJanuaryList.FirstOrDefault(p => p.Security.Trim().ToLower() == item.Security.Trim().ToLower()).Rate.ToString() : null);
                                            ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, closingRateForLongTerCapGainasJanuaryList.Any(p => p.Security.Trim().ToLower() == item.Security.Trim().ToLower()) ?
                                                closingRateForLongTerCapGainasJanuaryList.FirstOrDefault(p => p.Security.Trim().ToLower() == item.Security.Trim().ToLower()).Rate * item.SaleQty : null);

                                            fairMarketBuyValue = closingRateForLongTerCapGainasJanuaryList.Any(p => p.Security.Trim().ToLower() == item.Security.Trim().ToLower()) ?
                                                closingRateForLongTerCapGainasJanuaryList.FirstOrDefault(p => p.Security.Trim().ToLower() == item.Security.Trim().ToLower()).Rate * item.SaleQty : null;
                                        }
                                        else
                                        {
                                            ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, "NA");
                                            ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, "NA");
                                        }
                                    }

                                    if (groupedItem.Key.ToLower() != "buy back")
                                    {
                                        if (fairMarketBuyValue != null)
                                        {
                                            var tempAmount = Math.Max(item.BuyValue.Value, Math.Min(fairMarketBuyValue.Value, item.SaleValue.Value));

                                            if (tempAmount == item.SaleValue.Value)
                                            {
                                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, 0);
                                            }
                                            else
                                            {
                                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, item.SaleValue - (tempAmount + item.PurchaseBrokerage + item.PurchaseExchangeLevy
                                                                              + item.PurchaseServiceTax + item.PurchaseStampDuty
                                                                              + item.SaleBrokerage + item.SaleExchangeLevy +
                                                                              item.SaleServiceTax + item.SaleStampDuty));

                                                taxableProfit += (item.SaleValue - (tempAmount + item.PurchaseBrokerage + item.PurchaseExchangeLevy
                                                                              + item.PurchaseServiceTax + item.PurchaseStampDuty
                                                                              + item.SaleBrokerage + item.SaleExchangeLevy +
                                                                              item.SaleServiceTax + item.SaleStampDuty)).GetValueOrDefault();
                                            }
                                        }
                                        else
                                        {
                                            ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, item.SaleValue - (item.BuyValue + item.PurchaseBrokerage + item.PurchaseExchangeLevy
                                                                              + item.PurchaseServiceTax + item.PurchaseStampDuty
                                                                              + item.SaleBrokerage + item.SaleExchangeLevy +
                                                                              item.SaleServiceTax + item.SaleStampDuty));

                                            taxableProfit += (item.SaleValue - (item.BuyValue + item.PurchaseBrokerage + item.PurchaseExchangeLevy
                                                                              + item.PurchaseServiceTax + item.PurchaseStampDuty
                                                                              + item.SaleBrokerage + item.SaleExchangeLevy +
                                                                              item.SaleServiceTax + item.SaleStampDuty)).GetValueOrDefault();
                                        }

                                    }

                                    if (groupedItem.Key.ToLower() == "speculation")
                                        ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, Math.Abs(item.SaleValue.GetValueOrDefault() - item.BuyValue.GetValueOrDefault()));
                                    else if (groupedItem.Key.ToLower() == "long term" || groupedItem.Key.ToLower() == "short term")
                                        ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, ++i, Math.Abs(item.SaleValue.GetValueOrDefault()));

                                    //expenses
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, j++, item.PurchaseBrokerage);
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, j++, item.PurchaseServiceTax);
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, j++, item.PurchaseExchangeLevy);
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, j++, item.PurchaseStampDuty);
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, j++, item.PurchaseBrokerage + item.PurchaseExchangeLevy
                                                                      + item.PurchaseServiceTax + item.PurchaseStampDuty);

                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, j++, item.SaleBrokerage);
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, j++, item.SaleServiceTax);
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, j++, item.SaleExchangeLevy);
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, j++, item.SaleStampDuty);
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, j++, item.SaleBrokerage + item.SaleExchangeLevy
                                                                      + item.SaleServiceTax + item.SaleStampDuty);

                                    lastRow++;
                                    i = 1;
                                    j = 18;
                                }

                                i = 2;

                                for (; i < 8; i++)
                                    ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i, true, "grey");

                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i++, groupedItem.Sum(p => p.BuyValue));
                                ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i - 1, true, "grey");
                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i++, groupedItem.Sum(p => p.SaleValue));
                                ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i - 1, true, "grey");

                                summaryColumnIndex = 2;
                                ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex++, "Equity");
                                ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex++, groupedItem.Key);
                                ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex++, groupedItem.Sum(p => p.BuyValue));
                                ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex++, groupedItem.Sum(p => p.SaleValue));

                                var expense = groupedItem.Sum(p => p.PurchaseBrokerage + p.PurchaseExchangeLevy + p.PurchaseServiceTax + p.PurchaseStampDuty
                                                                      + p.SaleBrokerage + p.SaleExchangeLevy + p.SaleServiceTax + p.SaleStampDuty);
                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i++, expense);
                                ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i - 1, true, "grey");

                                ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex++, expense);

                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i++, groupedItem.Sum(p => p.SaleValue) - (groupedItem.Sum(p => p.BuyValue) + groupedItem.Sum(p => p.PurchaseBrokerage + p.PurchaseExchangeLevy
                                                                      + p.PurchaseServiceTax + p.PurchaseStampDuty + p.SaleBrokerage + p.SaleExchangeLevy + p.SaleServiceTax + p.SaleStampDuty)));
                                ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i - 1, true, "grey");

                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i++, "");
                                ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i - 1, true, "grey");

                                if (groupedItem.Key.ToLower() == "long term")
                                {
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i++, "");
                                    ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i - 1, true, "grey");

                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i++, "");
                                    ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i - 1, true, "grey");
                                }

                                if (groupedItem.Key.ToLower() != "long term")
                                {
                                    if (groupedItem.Key.ToLower() != "buy back")
                                    {
                                        ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i++, groupedItem.Sum(p => p.SaleValue) - (groupedItem.Sum(p => p.BuyValue) + groupedItem.Sum(p => p.PurchaseBrokerage + p.PurchaseExchangeLevy
                                                                      + p.PurchaseServiceTax + p.PurchaseStampDuty + p.SaleBrokerage + p.SaleExchangeLevy + p.SaleServiceTax + p.SaleStampDuty)));

                                        ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex++, groupedItem.Sum(p => p.SaleValue) - (groupedItem.Sum(p => p.BuyValue) + groupedItem.Sum(p => p.PurchaseBrokerage + p.PurchaseExchangeLevy
                                                                          + p.PurchaseServiceTax + p.PurchaseStampDuty + p.SaleBrokerage + p.SaleExchangeLevy + p.SaleServiceTax + p.SaleStampDuty)));
                                    }
                                        
                                }
                                else
                                {

                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i++, taxableProfit);
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, taxableProfitSummaryRow, taxableProfitSummarycolumn, taxableProfit);
                                    ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex++, taxableProfit);
                                }
                                ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i - 1, true, "grey");

                                if (groupedItem.Key.ToLower() == "speculation")
                                {
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i++, groupedItem.Sum(p => Math.Abs(p.SaleValue.GetValueOrDefault() - p.BuyValue.GetValueOrDefault())));
                                    ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i - 1, true, "grey");
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, turnOverSpeculationSummaryRow, turnOverSpeculationSummaryColumn + 1, groupedItem.Sum(p => Math.Abs(p.SaleValue.GetValueOrDefault() - p.BuyValue.GetValueOrDefault())));
                                }
                                else if (groupedItem.Key.ToLower() == "long term")
                                {
                                    var groupedByQuarter = groupedItem.GroupBy(item => GetQuarter(Convert.ToDateTime(item.TranDateSale)));
                                     EQLongresultArray = groupedByQuarter
                                      .Select(quarterGroup => new MyResult
                                      {
                                          Quarter = quarterGroup.Key,
                                          ProfitSum = quarterGroup.Sum(item => item.SaleValue - (item.BuyValue + item.PurchaseBrokerage + item.PurchaseExchangeLevy
                                                                    + item.PurchaseServiceTax + item.PurchaseStampDuty
                                                                    + item.SaleBrokerage + item.SaleExchangeLevy +
                                                                    item.SaleServiceTax + item.SaleStampDuty))
                                      })
                                      .ToArray();
                                    
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i++, Math.Abs(groupedItem.Sum(p => p.SaleValue.GetValueOrDefault())));
                                    ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i - 1, true, "grey");
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, taxableProfitSummaryRow, taxableProfitSummarycolumn + 1, Math.Abs(groupedItem.Sum(p => p.SaleValue.GetValueOrDefault())));
                                }
                                else if (groupedItem.Key.ToLower() == "short term")
                                {
                                    var groupedByQuarter = groupedItem.GroupBy(item => GetQuarter(Convert.ToDateTime(item.TranDateSale)));
                                    EQShortresultArray = groupedByQuarter
                                        .Select(quarterGroup => new MyResult
                                        {
                                            Quarter = quarterGroup.Key,
                                            ProfitSum = quarterGroup.Sum(item => item.SaleValue - (item.BuyValue + item.PurchaseBrokerage + item.PurchaseExchangeLevy
                                                                      + item.PurchaseServiceTax + item.PurchaseStampDuty
                                                                      + item.SaleBrokerage + item.SaleExchangeLevy +
                                                                      item.SaleServiceTax + item.SaleStampDuty))
                                        })
                                        .ToArray();                                  
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, i++, Math.Abs(groupedItem.Sum(p => p.SaleValue.GetValueOrDefault())));
                                    ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, i - 1, true, "grey");
                                    ExcelUtils.SetValueToCell(xlEquityWorkSheet, turnOverShortTermSummaryRow, turnOverShortTermSummaryColumn + 1, Math.Abs(groupedItem.Sum(p => p.SaleValue.GetValueOrDefault())));
                                }

                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, j++, groupedItem.Sum(p => p.PurchaseBrokerage));
                                ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, j - 1, true, "grey");

                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, j++, groupedItem.Sum(p => p.PurchaseServiceTax));
                                ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, j - 1, true, "grey");

                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, j++, groupedItem.Sum(p => p.PurchaseExchangeLevy));
                                ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, j - 1, true, "grey");

                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, j++, groupedItem.Sum(p => p.PurchaseStampDuty));
                                ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, j - 1, true, "grey");

                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, j++, groupedItem.Sum(p => p.PurchaseBrokerage + p.PurchaseExchangeLevy + p.PurchaseServiceTax + p.PurchaseStampDuty));
                                ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, j - 1, true, "grey");

                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, j++, groupedItem.Sum(p => p.SaleBrokerage));
                                ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, j - 1, true, "grey");

                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, j++, groupedItem.Sum(p => p.SaleServiceTax));
                                ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, j - 1, true, "grey");

                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, j++, groupedItem.Sum(p => p.SaleExchangeLevy));
                                ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, j - 1, true, "grey");

                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, j++, groupedItem.Sum(p => p.SaleStampDuty));
                                ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, j - 1, true, "grey");

                                ExcelUtils.SetValueToCell(xlEquityWorkSheet, lastRow, j++, groupedItem.Sum(p => p.SaleBrokerage + p.SaleExchangeLevy + p.SaleServiceTax + p.SaleStampDuty));
                                ExcelUtils.FormatCell(xlEquityWorkSheet, lastRow, j - 1, true, "grey");

                                lastRow += 2;
                                i = 0;
                                j = 18;
                                summaryLastRow++;
                            }
                        }
                    }
                    #endregion

                    #region F&O
                    var xlFOWorkSheet = wbook.Worksheets.Add("F&O Equity");
                    {
                        string[] summaryFOHeadings = { "Segment", "Type", "Buy Value", "Sell Value", "Net Profit/ Loss" };
                        ++summaryLastRow;
                        for (int k = 0; k < summaryFOHeadings.Length; k++)
                        {
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, k + 2, summaryFOHeadings[k]);
                            ExcelUtils.FormatCell(xlSummaryWorkSheet, summaryLastRow, k + 2, true);
                        }

                        using (GCCEntities gcc = new GCCEntities())
                        {
                            gcc.Database.CommandTimeout = queryTimeout;
                            var foResult = gcc.Database.SqlQuery<FO>($"SpTaxPandLExcel_V2 {input.RefId},{input.ClientId},'{input.FiscYear}','FO'").ToList();
                           //var foResult = gcc.Database.SqlQuery<FO>($"SpTaxPandLExcel_V2 {762845},{1290229308},'{2023-2024}','FO'").ToList();
                            var groupedByQuarter = foResult.GroupBy(item => GetQuarter(Convert.ToDateTime(item.TranDate)));
                             EQforesultArray = groupedByQuarter
                                .Select(quarterGroup => new MyResult
                                {
                                    Quarter = quarterGroup.Key,
                                    ProfitSum = quarterGroup.Sum(p => p.SQSaleValue - p.SQPurchaseValue)
                                })
                                .ToArray();
                            
                            string foSummaryHeadings = "Segment,Type,Buy Value,Sell Value,Net Profit & Loss, Turnover";
                            //string foOptionsHeadings = "Symbol,Contract,Strike,Option Type,Entry Date (Buy),Exit Date (Sell),Quantity,Buy Value,Sell Value,Net Profit/Loss,Turnover";
                            string foOptionsHeadings = "Symbol,Contract,Strike,Option Type,Quantity,Buy Value,Sell Value,Net Profit/Loss,Turnover";
                            //string foFutursHeadings = "Symbol,Description,Entry Date(Buy),Exit Date (Sell),Quantity,Buy Value,Sell Value, Net Profit/Loss,Turnover";
                            string foFuturesHeadings = "Symbol,Description,Quantity,Buy Value,Sell Value, Net Profit/Loss,Turnover";
                            i = 0;
                            lastRow = 6;
                            ExcelUtils.InsertPicture(xlFOWorkSheet, System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location) + "\\" + ConfigurationManager.AppSettings["ImagePath"], 0, 0);
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, i + 2, "Trade Code");
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, i + 3, client.CURLOCATION.Trim() + client.TRADECODE.Trim());
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlFOWorkSheet, ++lastRow, i + 2, "Client Name");
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, i + 3, client.NAME);
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlFOWorkSheet, ++lastRow, i + 2, "PAN");
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, i + 3, client.PAN_GIR);
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlFOWorkSheet, ++lastRow, i + 2, "Segment");
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, i + 3, "Equity F&O");
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, i + 3, false);


                            lastRow = 11;
                            string[] foSummayHeadingsArray = foSummaryHeadings.Split(',');
                            for (i = 0; i < foSummayHeadingsArray.Length; i++)
                            {
                                ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, i + 2, foSummayHeadingsArray[i]);
                                ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, i + 2, true);
                            }

                            var foOptions = foResult.Where(p => p.Instrument.StartsWith("OPT"));
                            var foFutures = foResult.Where(p => p.Instrument.StartsWith("FUT"));



                            lastRow += 1;
                            i = 1;
                            var buyValue = foOptions.Sum(p => p.SQPurchaseValue);
                            var sellValue = foOptions.Sum(p => p.SQSaleValue);
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, "Equity");
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, "Options");
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, buyValue);
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, sellValue);
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, sellValue - buyValue);
                            //ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, foOptions.Sum(p => p.SQSaleValue + Math.Abs(p.SQSaleValue - p.SQPurchaseValue)));
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, foOptions.Sum(p => Math.Abs(p.SQSaleValue - p.SQPurchaseValue)));

                            i = 1;
                            summaryLastRow++;
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, "Equity");
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, "Options");
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, buyValue);
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, sellValue);
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, sellValue - buyValue);



                            lastRow++;
                            i = 1;

                            buyValue = foFutures.Sum(p => p.SQPurchaseValue);
                            sellValue = foFutures.Sum(p => p.SQSaleValue);
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, "Equity");
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, "Futures");
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, buyValue);
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, sellValue);
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, sellValue - buyValue);
                            //ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, foFutures.Sum(p => Math.Abs(p.SQSaleValue)));
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, foFutures.Sum(p => Math.Abs(p.SQSaleValue-p.SQPurchaseValue)));

                            i = 1;
                            summaryLastRow++;
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, "Equity");
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, "Futures");
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, buyValue);
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, sellValue);
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, sellValue - buyValue);


                            i = 0;
                            lastRow = 15;
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, i + 2, "Total STT Paid");
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, i + 3, "Please refer attached 10DB form for details.");

                            lastRow = 17;
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, i + 2, "Tax Transaction Details from " + Convert.ToDateTime(start).ToString("dd-MM-yyyy") + " to " + Convert.ToDateTime(end).ToString("dd-MM-yyyy"));
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, i + 2, false);

                            lastRow = 19;
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow++, i + 2, "Options");
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow - 1, i + 2, false);

                            string[] foOptionsDataheadingsArray = foOptionsHeadings.Split(',');
                            for (i = 0; i < foOptionsDataheadingsArray.Length; i++)
                            {
                                ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, i + 2, foOptionsDataheadingsArray[i]);
                                ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, i + 2, true);
                            }

                            lastRow++;
                            i = 1;

                            foreach (var option in foOptions)
                            {
                                ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, option.Symbol);
                                ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, option.Contract);
                                ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, option.Strikeprice);
                                ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, option.OptionType);
                                ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, option.ActualSalesQty);
                                ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, option.SQPurchaseValue);
                                ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, option.SQSaleValue);
                                ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, option.SQSaleValue - option.SQPurchaseValue);
                                ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, Math.Abs(option.SQSaleValue - option.SQPurchaseValue));
                                i = 1;
                                lastRow++;
                            }

                            i = 1;
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, foOptions.Sum(p => p.SQPurchaseValue));
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, i, true, "grey");
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, foOptions.Sum(p => p.SQSaleValue));
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, i, true, "grey");
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, foOptions.Sum(p => p.SQSaleValue - p.SQPurchaseValue));
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, i, true, "grey");
                            //ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, foOptions.Sum(p => p.SQSaleValue + Math.Abs(p.SQSaleValue - p.SQPurchaseValue)));
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, foOptions.Sum(p => Math.Abs(p.SQSaleValue - p.SQPurchaseValue)));
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, i, true, "grey");

                            lastRow += 3;
                            i = 2;

                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow++, i, "Futures");
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow - 1, i, true);

                            string[] foFutursDataheadingsArray = foFuturesHeadings.Split(',');
                            for (i = 0; i < foFutursDataheadingsArray.Length; i++)
                            {
                                ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, i + 2, foFutursDataheadingsArray[i]);
                                ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, i + 2, true);
                            }

                            lastRow++;
                            i = 1;

                            foreach (var futures in foFutures)
                            {
                                ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, futures.Symbol);
                                ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, futures.Contract);
                                ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, futures.ActualSalesQty);
                                ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, futures.SQPurchaseValue);
                                ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, futures.SQSaleValue);
                                ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, futures.SQSaleValue - futures.SQPurchaseValue);
                                ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, Math.Abs(futures.SQSaleValue - futures.SQPurchaseValue));
                                i = 1;
                                lastRow++;
                            }

                            i = 1;
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, foFutures.Sum(p => p.SQPurchaseValue));
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, i, true, "grey");
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, foFutures.Sum(p => p.SQSaleValue));
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, i, true, "grey");
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, foFutures.Sum(p => p.SQSaleValue - p.SQPurchaseValue));
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, i, true, "grey");
                            //ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, foFutures.Sum(p => Math.Abs(p.SQSaleValue)));
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, ++i, foFutures.Sum(p => Math.Abs(p.SQSaleValue-p.SQPurchaseValue)));
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow, i, true, "grey");

                            lastRow += 2;
                            i = 2;
                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow++, i, "Note:");
                            ExcelUtils.FormatCell(xlFOWorkSheet, lastRow - 1, i, false);

                            ExcelUtils.SetValueToCell(xlFOWorkSheet, lastRow, i, "Purchase Value and Sales Value is Including all Expense");
                        }
                    }
                    #endregion

                    #region CDS
                    var xlCDSWorkSheet = wbook.Worksheets.Add("CDS");
                    {
                        using (GCCEntities gcc = new GCCEntities())
                        {
                            gcc.Database.CommandTimeout = queryTimeout;

                           var cdsResult = gcc.Database.SqlQuery<CDS>($"SpTaxPandLExcel_V2 {input.RefId},{input.ClientId},'{input.FiscYear}','CDS'").ToList();
                           // var cdsResult = gcc.Database.SqlQuery<CDS>($"SpTaxPandLExcel_V2 {653711},{1291054336},'{2022-2023}','CDS'").ToList();
                            
                            var groupedByQuarter = cdsResult.GroupBy(item => GetQuarter(Convert.ToDateTime(item.TranDate)));
                             EQcdsresultArray = groupedByQuarter
                              .Select(quarterGroup => new MyResult
                              {
                                  Quarter = quarterGroup.Key,
                                  ProfitSum = quarterGroup.Sum(p => p.SQSaleValue - p.SQPurchaseValue)
                              })
                              .ToArray();
                            
                            string cdsSummaryHeadings = "Segment,Type,Buy Value,Sell Value,Net Profit & Loss,Turnover";
                            //string cdsOptionsHeadings = "Symbol,Contract,Strike,Optio Type,Entry Date (Buy),Exit Date (Sell),Quantity,Buy Value,Sell Value,Net Profit/Loss,Turnover";
                            //string cdsFutursHeadings = "Symbol,Description,Entry Date(Buy),Exit Date (Sell),Quantity,Buy Value,Sell Value, Net Profit/Loss,Turnover";
                            string cdsOptionsHeadings = "Symbol,Contract,Strike,Option Type,Quantity,Buy Value,Sell Value,Net Profit/Loss,Turnover";
                            string cdsFuturesHeadings = "Symbol,Description,Quantity,Buy Value,Sell Value, Net Profit/Loss,Turnover";
                            i = 0;
                            lastRow = 6;
                            ExcelUtils.InsertPicture(xlCDSWorkSheet, System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location) + "\\" + ConfigurationManager.AppSettings["ImagePath"], 0, 0);

                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, i + 2, "Trade Code");
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, i + 3, client.CURLOCATION.Trim() + client.TRADECODE.Trim());
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, ++lastRow, i + 2, "Client Name");
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, i + 3, client.NAME);
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, ++lastRow, i + 2, "PAN");
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, i + 3, client.PAN_GIR);
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, ++lastRow, i + 2, "Segment");
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, i + 3, "Currency F&O");
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, i + 3, false);


                            lastRow = 11;
                            string[] cdsSummayHeadingsArray = cdsSummaryHeadings.Split(',');
                            for (i = 0; i < cdsSummayHeadingsArray.Length; i++)
                            {
                                ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, i + 2, cdsSummayHeadingsArray[i]);
                                ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, i + 2, true);
                            }

                            var cdsOptions = cdsResult.Where(p => p.Instrument.StartsWith("OPT"));
                            var cdsFutures = cdsResult.Where(p => p.Instrument.StartsWith("FUT"));



                            lastRow += 1;
                            i = 1;
                            var buyValue = cdsOptions.Sum(p => p.SQPurchaseValue);
                            var sellValue = cdsOptions.Sum(p => p.SQSaleValue);
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, "Currency");
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, "Options");
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, buyValue);
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, sellValue);
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, sellValue - buyValue);
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, cdsOptions.Sum(p => p.SQSaleValue + Math.Abs(p.SQSaleValue - p.SQPurchaseValue)));

                            i = 1;
                            summaryLastRow++;
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, "Currency");
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, "Options");
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, buyValue);
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, sellValue);
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, sellValue - buyValue);

                            lastRow++;
                            i = 1;

                            buyValue = cdsFutures.Sum(p => p.SQPurchaseValue);
                            sellValue = cdsFutures.Sum(p => p.SQSaleValue);
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, "Currency");
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, "Futures");
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, buyValue);
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, sellValue);
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, sellValue - buyValue);
                           // ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, cdsFutures.Sum(p => Math.Abs(p.SQSaleValue)));
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, cdsFutures.Sum(p => Math.Abs(p.SQSaleValue - p.SQPurchaseValue)));

                            i = 1;
                            summaryLastRow++;
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, "Currency");
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, "Futures");
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, buyValue);
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, sellValue);
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, sellValue - buyValue);


                            i = 0;
                            lastRow = 15;
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, i + 2, "Total STT Paid");
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, i + 3, "Please refer attached 10DB form for details.");

                            lastRow = 17;
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, i + 2, "Tax Transaction Details from " + Convert.ToDateTime(start).ToString("dd-MM-yyyy") + " to " + Convert.ToDateTime(end).ToString("dd-MM-yyyy"));
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, i + 2, true);

                            lastRow = 19;
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow++, i + 2, "Currency Options");
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow - 1, i + 2, false);

                            string[] cdsOptionsDataheadingsArray = cdsOptionsHeadings.Split(',');
                            for (i = 0; i < cdsOptionsDataheadingsArray.Length; i++)
                            {
                                ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, i + 2, cdsOptionsDataheadingsArray[i]);
                                ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, i + 2, true);
                            }

                            lastRow++;
                            i = 1;

                            foreach (var option in cdsOptions)
                            {
                                ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, option.Symbol);
                                ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, option.Contract);
                                ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, option.Strikeprice);
                                ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, option.OptionType);
                                ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, option.SQSaleQty);
                                ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, option.SQPurchaseValue);
                                ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, option.SQSaleValue);
                                ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, option.SQSaleValue - option.SQPurchaseValue);
                                ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, (option.SQSaleValue - option.SQPurchaseValue) > 0 ? option.SQSaleValue - option.SQPurchaseValue : option.SQPurchaseValue);
                                i = 1;
                                lastRow++;
                            }

                            i = 1;

                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, cdsOptions.Sum(p => p.SQPurchaseValue));
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, i, true, "grey");
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, cdsOptions.Sum(p => p.SQSaleValue));
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, i, true, "grey");
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, cdsOptions.Sum(p => p.SQSaleValue - p.SQPurchaseValue));
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, i, true, "grey");
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, cdsOptions.Sum(p => p.SQSaleValue + Math.Abs(p.SQSaleValue - p.SQPurchaseValue)));
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, i, true, "grey");

                            lastRow += 3;
                            i = 2;

                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow++, i, "Currency Futures");
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow - 1, i, false);

                            string[] foFutursDataheadingsArray = cdsFuturesHeadings.Split(',');
                            for (i = 0; i < foFutursDataheadingsArray.Length; i++)
                            {
                                ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, i + 2, foFutursDataheadingsArray[i]);
                                ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, i + 2, true);
                            }

                            lastRow++;
                            i = 1;

                            foreach (var futures in cdsFutures)
                            {
                                ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, futures.Symbol);
                                ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, futures.Contract);
                                ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, futures.SQSaleQty);
                                ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, futures.SQPurchaseValue);
                                ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, futures.SQSaleValue);
                                ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, futures.SQSaleValue - futures.SQPurchaseValue);
                                ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, Math.Abs(futures.SQSaleValue - futures.SQPurchaseValue));
                                i = 1;
                                lastRow++;
                            }

                            i = 1;

                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, cdsFutures.Sum(p => p.SQPurchaseValue));
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, i, true, "grey");
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, cdsFutures.Sum(p => p.SQSaleValue));
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, i, true, "grey");
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, cdsFutures.Sum(p => p.SQSaleValue - p.SQPurchaseValue));
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, i, true, "grey");
                            //ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, cdsFutures.Sum(p => Math.Abs(p.SQSaleValue)));
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, ++i, cdsFutures.Sum(p => Math.Abs(p.SQSaleValue - p.SQPurchaseValue)));
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow, i, true, "grey");

                            lastRow += 2;
                            i = 2;
                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow++, i, "Note:");
                            ExcelUtils.FormatCell(xlCDSWorkSheet, lastRow - 1, i, false);

                            ExcelUtils.SetValueToCell(xlCDSWorkSheet, lastRow, i, "Purchase Value and Sales Value is Including all Expense");
                        }
                    }
                    #endregion

                    #region Commodity
                    var xlCMWorkSheet = wbook.Worksheets.Add("Commodity");
                    {
                        using (GCCEntities gcc = new GCCEntities())
                        {
                            gcc.Database.CommandTimeout = queryTimeout;

                           //var cmResult = gcc.Database.SqlQuery<CDS>($"SpTaxPandLExcel_V2 {input.RefId},{input.ClientId},'{input.FiscYear}','Commodity'").ToList();
                            var cmResult = gcc.Database.SqlQuery<CDS>($"SpTaxPandLExcel_V2 {761162},{1291561587},'{2022 - 2023}','Commodity'").ToList();
                            
                              
                            var groupedByQuarter = cmResult.GroupBy(item => GetQuarter(Convert.ToDateTime(item.TranDate)));
                             EQcmresultArray = groupedByQuarter
                              .Select(quarterGroup => new MyResult
                              {
                                  Quarter = quarterGroup.Key,
                                  ProfitSum = quarterGroup.Sum(p => p.SQSaleValue - p.SQPurchaseValue)
                              })
                              .ToArray();

                            
                            string cmdSummaryHeadings = "Segment,Type,Buy Value,Sell Value,Net Profit & Loss,Turnover";                 
                            string cmdOptionsHeadings = "Symbol,Contract,Strike,Option Type,Quantity,Buy Value,Sell Value,Net Profit/Loss,Turnover";
                            string cmdFuturesHeadings = "Symbol,Description,Quantity,Buy Value,Sell Value, Net Profit/Loss,Turnover";
                            i = 0;
                            lastRow = 6;
                            ExcelUtils.InsertPicture(xlCMWorkSheet, System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location) + "\\" + ConfigurationManager.AppSettings["ImagePath"], 0, 0);

                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, i + 2, "Trade Code");
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, i + 3, client.CURLOCATION.Trim() + client.TRADECODE.Trim());
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlCMWorkSheet, ++lastRow, i + 2, "Client Name");
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, i + 3, client.NAME);
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlCMWorkSheet, ++lastRow, i + 2, "PAN");
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, i + 3, client.PAN_GIR);
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlCMWorkSheet, ++lastRow, i + 2, "Segment");
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, i + 3, "Currency F&O");
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, i + 3, false);


                            lastRow = 11;
                            string[] cmdSummayHeadingsArray = cmdSummaryHeadings.Split(',');
                            for (i = 0; i < cmdSummayHeadingsArray.Length; i++)
                            {
                                ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, i + 2, cmdSummayHeadingsArray[i]);
                                ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, i + 2, true);
                            }

                            var cmOptions = cmResult.Where(p => p.Instrument.StartsWith("OPT"));
                            var cmFutures = cmResult.Where(p => p.Instrument.StartsWith("FUT"));



                            lastRow += 1;
                            i = 1;
                            var buyValue = cmOptions.Sum(p => p.SQPurchaseValue);
                            var sellValue = cmOptions.Sum(p => p.SQSaleValue);
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, "Commodity");
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, "Options");
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, buyValue);
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, sellValue);
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, sellValue - buyValue);
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, cmOptions.Sum(p => p.SQSaleValue + Math.Abs(p.SQSaleValue - p.SQPurchaseValue)));

                            i = 1;
                            summaryLastRow++;
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, "Commodity");
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, "Options");
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, buyValue);
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, sellValue);
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, sellValue - buyValue);

                            lastRow++;
                            i = 1;

                            buyValue = cmFutures.Sum(p => p.SQPurchaseValue);
                            sellValue = cmFutures.Sum(p => p.SQSaleValue);
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, "Commodity");
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, "Futures");
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, buyValue);
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, sellValue);
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, sellValue - buyValue);
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, cmFutures.Sum(p => Math.Abs(p.SQSaleValue - p.SQPurchaseValue)));

                            i = 1;
                            summaryLastRow++;
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, "Commodity");
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, "Futures");
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, buyValue);
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, sellValue);
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, ++i, sellValue - buyValue);


                            i = 0;
                            lastRow = 15;
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, i + 2, "Total STT Paid");
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, i + 3, "Please refer attached 10DB form for details.");

                            lastRow = 17;
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, i + 2, "Tax Transaction Details from " + Convert.ToDateTime(start).ToString("dd-MM-yyyy") + " to " + Convert.ToDateTime(end).ToString("dd-MM-yyyy"));
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, i + 2, true);

                            lastRow = 19;
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow++, i + 2, "Commodity Options");
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow - 1, i + 2, false);

                            string[] cdsOptionsDataheadingsArray = cmdOptionsHeadings.Split(',');
                            for (i = 0; i < cdsOptionsDataheadingsArray.Length; i++)
                            {
                                ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, i + 2, cdsOptionsDataheadingsArray[i]);
                                ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, i + 2, true);
                            }

                            lastRow++;
                            i = 1;

                            foreach (var option in cmOptions)
                            {
                                ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, option.Symbol);
                                ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, option.Contract);
                                ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, option.Strikeprice);
                                ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, option.OptionType);
                                ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, option.SQSaleQty);
                                ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, option.SQPurchaseValue);
                                ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, option.SQSaleValue);
                                ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, option.SQSaleValue - option.SQPurchaseValue);
                                ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, (option.SQSaleValue - option.SQPurchaseValue) > 0 ? option.SQSaleValue - option.SQPurchaseValue : option.SQPurchaseValue);
                                i = 1;
                                lastRow++;
                            }

                            i = 1;

                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, cmOptions.Sum(p => p.SQPurchaseValue));
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, i, true, "grey");
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, cmOptions.Sum(p => p.SQSaleValue));
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, i, true, "grey");
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, cmOptions.Sum(p => p.SQSaleValue - p.SQPurchaseValue));
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, i, true, "grey");
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, cmOptions.Sum(p => p.SQSaleValue + Math.Abs(p.SQSaleValue - p.SQPurchaseValue)));
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, i, true, "grey");

                            lastRow += 3;
                            i = 2;

                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow++, i, "Commodity Futures");
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow - 1, i, false);

                            string[] foFutursDataheadingsArray = cmdFuturesHeadings.Split(',');
                            for (i = 0; i < foFutursDataheadingsArray.Length; i++)
                            {
                                ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, i + 2, foFutursDataheadingsArray[i]);
                                ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, i + 2, true);
                            }

                            lastRow++;
                            i = 1;

                            foreach (var futures in cmFutures)
                            {
                                ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, futures.Symbol);
                                ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, futures.Contract);
                                ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, futures.SQSaleQty);
                                ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, futures.SQPurchaseValue);
                                ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, futures.SQSaleValue);
                                ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, futures.SQSaleValue - futures.SQPurchaseValue);
                                ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, Math.Abs(futures.SQSaleValue - futures.SQPurchaseValue));
                                i = 1;
                                lastRow++;
                            }

                            i = 1;

                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, ++i, true, "grey");
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, cmFutures.Sum(p => p.SQPurchaseValue));
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, i, true, "grey");
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, cmFutures.Sum(p => p.SQSaleValue));
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, i, true, "grey");
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, cmFutures.Sum(p => p.SQSaleValue - p.SQPurchaseValue));
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, i, true, "grey");
                            //ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, cdsFutures.Sum(p => Math.Abs(p.SQSaleValue)));
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, ++i, cmFutures.Sum(p => Math.Abs(p.SQSaleValue - p.SQPurchaseValue)));
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow, i, true, "grey");

                            lastRow += 2;
                            i = 2;
                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow++, i, "Note:");
                            ExcelUtils.FormatCell(xlCMWorkSheet, lastRow - 1, i, false);

                            ExcelUtils.SetValueToCell(xlCMWorkSheet, lastRow, i, "Purchase Value and Sales Value is Including all Expense");
                        }
                    }
                    #endregion
                    #region Bond
                    var xlBondWorkSheet = wbook.Worksheets.Add("Bond");
                    {
                        string[] summaryBOHeadings = { "Segment", "Buy Value", "Sell Value", "Net Profit/ Loss" };
                        summaryLastRow += 2;
                        for (int k = 0; k < summaryBOHeadings.Length; k++)
                        {
                            ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, k + 2, summaryBOHeadings[k]);
                            ExcelUtils.FormatCell(xlSummaryWorkSheet, summaryLastRow, k + 2, true);
                        }
                        summaryLastRow++;
                        using (GCCEntities gcc = new GCCEntities())
                        {
                            gcc.Database.CommandTimeout = queryTimeout;

                            var Bonddataheadings = "Symbol,Description,ISIN,Entry Date (Buy),Exit Date (Sell),Quantity,Buy Value,Sell Value,Buy & Sell Expense ,Net Profit/Loss ,Period of Holding (Days)";
                            var BondSummayHeadings = "Segment,Type,Buy Value,Sell Value,Expense,Net Profit/ Loss";
                            var BondtaxHeadings = "Buy Brokerage,Buy GST,Buy Exchange Levy,Buy Stamp Duty,Total Buy Cost,Sale Bokerage,Sale GST,Sale Exchange Levy,Sale Stamp Duty,Total Sell Cost";
                            string[] bondSummayHeadingsArray = BondSummayHeadings.Split(',');

                            var BondResult = gcc.Database.SqlQuery<ModelBond>($"SpTaxPandLExcel_V2 {input.RefId},{input.ClientId},'{input.FiscYear}','Bond'").ToList();
                            //var BondResult = gcc.Database.SqlQuery<ModelBond>($"SpTaxPandLExcel_V2 {622718},{1290789020},'{2022 - 2023}','Bond'").ToList();
                            var groupedData = BondResult.GroupBy(p => p.Categ_desc);


                            var groupedByQuarter = BondResult.GroupBy(item => GetQuarter(Convert.ToDateTime(item.TranDateSale)));
                            EQboresultArray = groupedByQuarter
                             .Select(quarterGroup => new MyResult
                             {
                                 Quarter = quarterGroup.Key,
                                 ProfitSum = quarterGroup.Sum(item => item.SaleValue - (item.BuyValue + item.PurchaseBrokerage + item.PurchaseExchangeLevy
                                                                    + item.PurchaseServiceTax + item.PurchaseStampDuty
                                                                    + item.SaleBrokerage + item.SaleExchangeLevy +
                                                                    item.SaleServiceTax + item.SaleStampDuty))

                             })
                             .ToArray();

                            ExcelUtils.InsertPicture(xlBondWorkSheet, System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location) + "\\" + ConfigurationManager.AppSettings["ImagePath"], 1, 1);
                            i = 0;
                            lastRow = 6;

                            ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, i + 2, "Trade Code");
                            ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, i + 3, client.CURLOCATION.Trim() + client.TRADECODE.Trim());
                            ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlBondWorkSheet, ++lastRow, i + 2, "Client Name");
                            ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, i + 3, client.NAME);
                            ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlBondWorkSheet, ++lastRow, i + 2, "PAN");
                            ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, i + 3, client.PAN_GIR);
                            ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlBondWorkSheet, ++lastRow, i + 2, "Segment");
                            ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, i + 3, "Bond/NCD/Liquid & Commodity ETF");
                            ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, i + 3, false);

                            lastRow = 11;
                            for (i = 0; i < bondSummayHeadingsArray.Length; i++)
                            {
                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, i + 2, bondSummayHeadingsArray[i]);
                                ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, i + 2, true);
                            }

                            lastRow += 1;
                            i = 1;
                            var taxablebondProfitSummaryRow = 0;
                            var taxablebondProfitSummarycolumn = 0;

                            var turnOverGovSummaryRow = 0;
                            var turnOverGovSummaryColumn = 0;



                            foreach (var group in groupedData)
                            {
                                var totalExpense = group.Sum(p => p.PurchaseBrokerage + p.PurchaseExchangeLevy + p.PurchaseServiceTax + p.PurchaseStampDuty
                                                                      + p.SaleBrokerage + p.SaleExchangeLevy + p.SaleServiceTax + p.SaleStampDuty);
                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, ++i, "Equity");
                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, ++i, group.Key);
                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, ++i, group.Sum(p => p.BuyValue));
                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, ++i, group.Sum(p => p.SaleValue));
                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, ++i, totalExpense);
                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, ++i, group.Sum(p => p.SaleValue) - (group.Sum(p => p.BuyValue) + group.Sum(p => p.PurchaseBrokerage + p.PurchaseExchangeLevy
                                                                      + p.PurchaseServiceTax + p.PurchaseStampDuty + p.SaleBrokerage + p.SaleExchangeLevy + p.SaleServiceTax + p.SaleStampDuty)));

                                lastRow++;
                                i = 1;
                            }

                            i = 0;
                            lastRow = 17;
                            ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, i + 2, "Total STT Paid");
                            ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, i + 3, "Please refer attached 10DB form for details.");

                            lastRow = 19;
                            ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, i + 2, "Trade wise exits from " + Convert.ToDateTime(start).ToString("dd-MM-yyyy") + " to " + Convert.ToDateTime(end).ToString("dd-MM-yyyy"));
                            ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, i + 2, false);

                            lastRow = 21;

                            GCCEntities gCCEntities = new GCCEntities();
                            var closingRateForLongTerCapGainasJanuaryList = gCCEntities.ClosingRateForLongTerCapGainasJanuaries.ToList();
                            foreach (var groupedItem in groupedData)
                            {

                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, i + 2, groupedItem.Key);
                                ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, i + 2, false);
                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, 18, "Expense Breakup");
                                ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, 18, false);
                                lastRow++;

                                string[] bonddataheadingsArray = Bonddataheadings.Split(',');
                                string[] bondtaxHeadingsArray = BondtaxHeadings.Split(',');

                                for (i = 0, j = 18; i < bonddataheadingsArray.Length; i++)
                                {
                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, i + 2, bonddataheadingsArray[i]);
                                    ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, i + 2, true);
                                }
                                for (i = 0, j = 18; i < bondtaxHeadingsArray.Length; i++)
                                {
                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, j++, bondtaxHeadingsArray[i]);
                                    ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, j - 1, true);
                                }

                                lastRow++;
                                i = 1;
                                j = 18;
                                decimal taxableProfit = 0;

                                foreach (var item in groupedItem)
                                {
                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, ++i, item.Security);
                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, ++i, item.Description);
                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, ++i, item.ISIN);
                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, ++i, item.TranDateBuy);
                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, ++i, item.TranDateSale);
                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, ++i, item.SaleQty);
                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, ++i, item.BuyValue);
                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, ++i, item.SaleValue);
                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, ++i, item.PurchaseBrokerage + item.PurchaseExchangeLevy
                                                                      + item.PurchaseServiceTax + item.PurchaseStampDuty
                                                                      + item.SaleBrokerage + item.SaleExchangeLevy +
                                                                      item.SaleServiceTax + item.SaleStampDuty);

                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, ++i, item.SaleValue - (item.BuyValue + item.PurchaseBrokerage + item.PurchaseExchangeLevy
                                                                      + item.PurchaseServiceTax + item.PurchaseStampDuty
                                                                      + item.SaleBrokerage + item.SaleExchangeLevy +
                                                                      item.SaleServiceTax + item.SaleStampDuty));
                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, ++i, item.DayToSell.HasValue && item.DayToSell > 0 ? item.DayToSell : 0);


                                    //expenses
                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, j++, item.PurchaseBrokerage);
                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, j++, item.PurchaseServiceTax);
                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, j++, item.PurchaseExchangeLevy);
                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, j++, item.PurchaseStampDuty);
                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, j++, item.PurchaseBrokerage + item.PurchaseExchangeLevy
                                                                      + item.PurchaseServiceTax + item.PurchaseStampDuty);

                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, j++, item.SaleBrokerage);
                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, j++, item.SaleServiceTax);
                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, j++, item.SaleExchangeLevy);
                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, j++, item.SaleStampDuty);
                                    ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, j++, item.SaleBrokerage + item.SaleExchangeLevy
                                                                      + item.SaleServiceTax + item.SaleStampDuty);

                                    lastRow++;
                                    i = 1;
                                    j = 18;
                                }

                                i = 2;

                                for (; i < 8; i++)
                                    ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, i, true, "grey");

                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, i++, groupedItem.Sum(p => p.BuyValue));
                                ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, i - 1, true, "grey");
                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, i++, groupedItem.Sum(p => p.SaleValue));
                                ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, i - 1, true, "grey");

                                summaryColumnIndex = 2;
                                ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex++, groupedItem.Key);
                                ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex++, groupedItem.Sum(p => p.BuyValue));
                                ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex++, groupedItem.Sum(p => p.SaleValue));

                                var expense = groupedItem.Sum(p => p.PurchaseBrokerage + p.PurchaseExchangeLevy + p.PurchaseServiceTax + p.PurchaseStampDuty
                                                                      + p.SaleBrokerage + p.SaleExchangeLevy + p.SaleServiceTax + p.SaleStampDuty);
                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, i++, expense);
                                ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, i - 1, true, "grey");
                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, i++, groupedItem.Sum(p => p.SaleValue) - (groupedItem.Sum(p => p.BuyValue) + groupedItem.Sum(p => p.PurchaseBrokerage + p.PurchaseExchangeLevy
                                                                      + p.PurchaseServiceTax + p.PurchaseStampDuty + p.SaleBrokerage + p.SaleExchangeLevy + p.SaleServiceTax + p.SaleStampDuty)));
                                ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, summaryColumnIndex++, groupedItem.Sum(p => p.SaleValue) - (groupedItem.Sum(p => p.BuyValue) + groupedItem.Sum(p => p.PurchaseBrokerage + p.PurchaseExchangeLevy
                                                                      + p.PurchaseServiceTax + p.PurchaseStampDuty + p.SaleBrokerage + p.SaleExchangeLevy + p.SaleServiceTax + p.SaleStampDuty)));
                                ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, i - 1, true, "grey");
                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, i++, "");
                                ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, i - 1, true, "grey");
                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, j++, groupedItem.Sum(p => p.PurchaseBrokerage));
                                ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, j - 1, true, "grey");

                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, j++, groupedItem.Sum(p => p.PurchaseServiceTax));
                                ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, j - 1, true, "grey");

                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, j++, groupedItem.Sum(p => p.PurchaseExchangeLevy));
                                ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, j - 1, true, "grey");

                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, j++, groupedItem.Sum(p => p.PurchaseStampDuty));
                                ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, j - 1, true, "grey");

                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, j++, groupedItem.Sum(p => p.PurchaseBrokerage + p.PurchaseExchangeLevy + p.PurchaseServiceTax + p.PurchaseStampDuty));
                                ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, j - 1, true, "grey");

                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, j++, groupedItem.Sum(p => p.SaleBrokerage));
                                ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, j - 1, true, "grey");

                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, j++, groupedItem.Sum(p => p.SaleServiceTax));
                                ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, j - 1, true, "grey");

                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, j++, groupedItem.Sum(p => p.SaleExchangeLevy));
                                ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, j - 1, true, "grey");

                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, j++, groupedItem.Sum(p => p.SaleStampDuty));
                                ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, j - 1, true, "grey");

                                ExcelUtils.SetValueToCell(xlBondWorkSheet, lastRow, j++, groupedItem.Sum(p => p.SaleBrokerage + p.SaleExchangeLevy + p.SaleServiceTax + p.SaleStampDuty));
                                ExcelUtils.FormatCell(xlBondWorkSheet, lastRow, j - 1, true, "grey");

                                lastRow += 2;
                                i = 0;
                                j = 18;
                                summaryLastRow++;
                            }
                        }
                    }
                    #endregion


                    #region OtherCharges
                    var xlOCWorkSheet = wbook.Worksheets.Add("Other Charges");
                    {
                        using (GCCEntities gcc = new GCCEntities())
                        {
                            gcc.Database.CommandTimeout = queryTimeout;

                            var OCSummayHeadings = "Particular,Amount";
                            string[] OCSummayHeadingsArray = OCSummayHeadings.Split(',');


                            var otherchargeResult = gcc.Database.SqlQuery<OtherCharges>($"SpOtherChargePandL_V1 {input.ClientId},'{input.FiscYear}'").ToList();
                            ExcelUtils.InsertPicture(xlOCWorkSheet, System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location) + "\\" + ConfigurationManager.AppSettings["ImagePath"], 1, 1);
                            i = 0;
                            lastRow = 6;

                            ExcelUtils.SetValueToCell(xlOCWorkSheet, lastRow, i + 2, "Trade Code");
                            ExcelUtils.FormatCell(xlOCWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlOCWorkSheet, lastRow, i + 3, client.CURLOCATION.Trim() + client.TRADECODE.Trim());
                            ExcelUtils.FormatCell(xlOCWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlOCWorkSheet, ++lastRow, i + 2, "Client Name");
                            ExcelUtils.FormatCell(xlOCWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlOCWorkSheet, lastRow, i + 3, client.NAME);
                            ExcelUtils.FormatCell(xlOCWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlOCWorkSheet, ++lastRow, i + 2, "PAN");
                            ExcelUtils.FormatCell(xlOCWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlOCWorkSheet, lastRow, i + 3, client.PAN_GIR);
                            ExcelUtils.FormatCell(xlOCWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlOCWorkSheet, ++lastRow, i + 2, "Segment");
                            ExcelUtils.FormatCell(xlOCWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlOCWorkSheet, lastRow, i + 3, "Commodity");
                            ExcelUtils.FormatCell(xlOCWorkSheet, lastRow, i + 3, false);


                            i = 0;
                            lastRow = 11;

                            ExcelUtils.SetValueToCell(xlOCWorkSheet, lastRow, i + 2, "Other Charges  " + Convert.ToDateTime(start).ToString("dd-MM-yyyy") + " to " + Convert.ToDateTime(end).ToString("dd-MM-yyyy") + "(Not included in P&L Calculation)");
                            ExcelUtils.FormatCell(xlOCWorkSheet, lastRow, i + 2, false);

                            lastRow = 14;
                            for (i = 0; i < OCSummayHeadingsArray.Length; i++)
                            {
                                ExcelUtils.SetValueToCell(xlOCWorkSheet, lastRow, i + 2, OCSummayHeadingsArray[i]);
                                ExcelUtils.FormatCell(xlOCWorkSheet, lastRow, i + 2, true,"DarkSeaGreen");
                                xlOCWorkSheet.Column(i + 2).Width = 30;
                                var range = xlOCWorkSheet.Range(lastRow, i + 2, lastRow, i + 2);
                                range.Merge().Value = OCSummayHeadingsArray[i];
                                range.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
                                range.Style.Border.TopBorder = XLBorderStyleValues.Thin;
                                range.Style.Border.BottomBorder = XLBorderStyleValues.Thin;
                                range.Style.Border.LeftBorder = XLBorderStyleValues.Thin;
                                range.Style.Border.RightBorder = XLBorderStyleValues.Thin;
                            }

                            lastRow += 1;
                            i = 1;


                            foreach (var item in otherchargeResult)
                            {
                                
                                if(item.Particulars != "Margin Requirement(opening)"&&item.Particulars !="Margin Requirement(closing)")
                                {
                                    ExcelUtils.SetValueToCell(xlOCWorkSheet, lastRow, ++i, item.Particulars);
                                    ExcelUtils.SetValueToCell(xlOCWorkSheet, lastRow, ++i, item.Amount);
                                    var range = xlOCWorkSheet.Range(lastRow, 2, lastRow, 3);
                                    xlOCWorkSheet.Column(2).Width = 30;
                                    range.FirstColumn().Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Left;
                                    range.LastColumn().Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Right;

                                    //range.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
                                    range.Style.Border.TopBorder = XLBorderStyleValues.Thin;
                                    range.Style.Border.BottomBorder = XLBorderStyleValues.Thin;
                                    range.Style.Border.LeftBorder = XLBorderStyleValues.Thin;
                                    range.Style.Border.RightBorder = XLBorderStyleValues.Thin;
                                    lastRow++;
                                    i = 1;
                                }
                                else
                                {
                                   
                                    mgopenvalue = item.Particulars == "Margin Requirement(opening)" ? item.Particulars : "0";
                                    mgcloseval = item.Particulars == "Margin Requirement(closing)" ? item.Particulars : "0";
                                }
                               
                            }

                            var totalAmount = otherchargeResult.Sum(item => item.Amount);
                            ExcelUtils.SetValueToCell(xlOCWorkSheet, lastRow, 2, "");
                            ExcelUtils.SetValueToCell(xlOCWorkSheet, lastRow, 3, totalAmount);
                            ExcelUtils.FormatCell(xlOCWorkSheet, lastRow, 2, true, "DarkSeaGreen");
                            ExcelUtils.FormatCell(xlOCWorkSheet, lastRow, 3, true, "DarkSeaGreen");
                            var ranges = xlOCWorkSheet.Range(lastRow, 2, lastRow, 3);
                            xlOCWorkSheet.Column(2).Width = 30;
                            ranges.FirstColumn().Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Left;
                            ranges.LastColumn().Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Right;

                            ranges.Style.Border.TopBorder = XLBorderStyleValues.Thin;
                            ranges.Style.Border.BottomBorder = XLBorderStyleValues.Thin;
                            ranges.Style.Border.LeftBorder = XLBorderStyleValues.Thin;
                            ranges.Style.Border.RightBorder = XLBorderStyleValues.Thin;

                        }
                    }
                    #endregion

                    #region Opening Pos
                    var xlOpeningPosWorkSheet = wbook.Worksheets.Add("Opening Pos(" + Convert.ToDateTime(start).ToString("dd-MM-yyyy") + ")");
                    {
                        using (GCCEntities gcc = new GCCEntities())
                        {
                            gcc.Database.CommandTimeout = queryTimeout;
                            DateTime currentDate = DateTime.Now;
                            int financialYearStartMonth = 4;
                            var finyear = input.FiscYear;                            
                            start = finyear.Split('-')[0] + "-04-01";
                            end = finyear.Split('-')[1] + "-03-31";
                            var openend  = finyear.Split('-')[0] + "-03-31";
                            var Optionheadings = "Symbol,Contract,Strike,Option Type,Trade Date,Open Qty,Avg Price,Value,Closing Price(31st March)";
                            var Futureheadings = "Symbol,Description,Trade Date,Open Qty,Avg Price,Value,Closing Price(31st March)";
                            var posResult = gcc.Database.SqlQuery<Pos>($"SpGetOpenPositionDetails '{input.ClientId}','{start}','{end}'").ToList();                          
                           //var posResult = gcc.Database.SqlQuery<Pos>($"SpGetOpenPositionDetails '{1290934839}','2023-04-01','2024-03-31'").ToList();
                            var groupedposData = posResult.GroupBy(p => p.Type).OrderByDescending(group => group.Key).ToList(); ;
                            ExcelUtils.InsertPicture(xlOpeningPosWorkSheet, System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location) + "\\" + ConfigurationManager.AppSettings["ImagePath"], 1, 1);
                            i = 0;
                            lastRow = 6;
                            ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, i + 2, "Trade Code");
                            ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, i + 3, client.CURLOCATION.Trim() + client.TRADECODE.Trim());
                            ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, ++lastRow, i + 2, "Client Name");
                            ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, i + 3, client.NAME);
                            ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, ++lastRow, i + 2, "PAN");
                            ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, i + 3, client.PAN_GIR);
                            ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, ++lastRow, i + 2, "Segment");
                            ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, i + 3, "Equity");
                            ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, i + 3, false);

                            i = 1;

                            lastRow = 11;
                            ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, i + 2, "Margin requirement:   ");
                            ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, i + 3, mgopenvalue);

                            lastRow = 13;
                            ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, i + 1, "Open Position as on " + Convert.ToDateTime(openend).ToString("dd-MM-yyyy") + "(PY Closing Position)");
                            ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, i + 1, false);

                            lastRow = 15;

                            GCCEntities gCCEntities = new GCCEntities();
                            var subheading = "";
                            var closingRateForLongTerCapGainasJanuaryList = gCCEntities.ClosingRateForLongTerCapGainasJanuaries.ToList();
                            string[] OptionheadingsArray = Optionheadings.Split(',');
                            string[] FutureheadingsArray = Futureheadings.Split(',');
                            if (groupedposData.Any())
                            {
                                foreach (var groupedItem in groupedposData)
                                {
                                    i = 0;
                                    var foFutures = groupedItem.Select(group => group).Where(p => p.Instrument.StartsWith("FUT"));
                                    var foOptions = groupedItem.Select(group => group).Where(p => p.Instrument.StartsWith("OPT"));
                                    subheading = (groupedItem.Key.ToLower() == "eq") ? "Equity" : (groupedItem.Key.ToLower() == "cur") ? "Currency" : "Commodity";
                                    ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, i + 2, subheading + "(Options)");
                                    ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, i + 2, false);

                                    lastRow++;
                                    i = 0;
                                    for (i = 0; i < OptionheadingsArray.Length; i++)
                                    {
                                        ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, i + 2, OptionheadingsArray[i]);
                                        ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, i + 2, true);
                                    }

                                    lastRow++;
                                    i = 1;

                                    foreach (var option in foOptions)
                                    {
                                        ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, ++i, option.Symbol);
                                        ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, ++i, option.Contract);
                                        ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, ++i, option.Strikeprice);
                                        ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, ++i, option.OptionType);
                                        ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, ++i, option.TradeDate.Date);
                                        ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, ++i, option.OpenQty);
                                        ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, ++i, option.AvgRate);
                                        ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, ++i, option.AvgRate * option.OpenQty);
                                        ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, ++i, option.ClosingRate);
                                        i = 1;
                                        lastRow++;
                                    }

                                    i = 1;
                                    ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, ++i, foOptions.Sum(p => p.AvgRate * p.OpenQty));
                                    ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, i, true, "grey");
                                    ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, ++i, true, "grey");




                                    lastRow += 3;
                                    i = 2;
                                    ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow++, i, subheading + "(Futures)");
                                    ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow - 1, i, true);
                                    for (i = 0; i < FutureheadingsArray.Length; i++)
                                    {
                                        ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, i + 2, FutureheadingsArray[i]);
                                        ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, i + 2, true);
                                    }

                                    lastRow++;
                                    i = 1;
                                    foreach (var futures in foFutures)
                                    {
                                        ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, ++i, futures.Symbol);
                                        ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, ++i, futures.Contract);
                                        ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, ++i, futures.TradeDate.Date);
                                        ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, ++i, futures.OpenQty);
                                        ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, ++i, futures.AvgRate);
                                        ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, ++i, futures.AvgRate * futures.OpenQty);
                                        ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, ++i, futures.ClosingRate);
                                        i = 1;
                                        lastRow++;
                                    }
                                    ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.SetValueToCell(xlOpeningPosWorkSheet, lastRow, ++i, foFutures.Sum(p => p.AvgRate * p.OpenQty));
                                    ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, i, true, "grey");
                                    ExcelUtils.FormatCell(xlOpeningPosWorkSheet, lastRow, ++i, true, "grey");
                                    lastRow += 3;
                                }
                            }

                        }

                    }
                    #endregion


                    #region Closing Pos
                    var xlClosingPosWorkSheet = wbook.Worksheets.Add("Closing Pos(" + Convert.ToDateTime(end).ToString("dd-MM-yyyy") + ")");
                    {
                        using (GCCEntities gcc = new GCCEntities())
                        {
                            gcc.Database.CommandTimeout = queryTimeout;
                            DateTime currentDate = DateTime.Now;
                            int financialYearStartMonth = 4;
                            var finyear = input.FiscYear;                           
                            start = finyear.Split('-')[0] + "-04-01";
                            end = finyear.Split('-')[1] + "-03-31";
                            var Optionheadings = "Symbol,Contract,Strike,Option Type,Trade Date,Open Qty,Avg Price,Value,Closing Price(31st March)";
                            var Futureheadings = "Symbol,Description,Trade Date,Open Qty,Avg Price,Value,Closing Price(31st March)";
                            var posResult = gcc.Database.SqlQuery<Pos>($"SpGetClosedPositionDetails '{input.ClientId}','{start}','{end}'").ToList();
                            var groupedposData = posResult.GroupBy(p => p.Type).OrderByDescending(group => group.Key).ToList(); ;
                            ExcelUtils.InsertPicture(xlClosingPosWorkSheet, System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location) + "\\" + ConfigurationManager.AppSettings["ImagePath"], 1, 1);
                            i = 0;
                            lastRow = 6;
                            ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, i + 2, "Trade Code");
                            ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, i + 3, client.CURLOCATION.Trim() + client.TRADECODE.Trim());
                            ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, ++lastRow, i + 2, "Client Name");
                            ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, i + 3, client.NAME);
                            ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, ++lastRow, i + 2, "PAN");
                            ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, i + 3, client.PAN_GIR);
                            ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, ++lastRow, i + 2, "Segment");
                            ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, i + 3, "Equity");
                            ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, i + 3, false);

                            i = 1;
                            lastRow = 11;
                            ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, i + 2, "Margin requirement:   ");
                            ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, i + 3, mgcloseval);

                            lastRow = 13;
                            ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, i + 1, "Open Position as on " + Convert.ToDateTime(end).ToString("dd-MM-yyyy") + "(Current year Closing Position)");
                            ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, i + 1, false);

                            lastRow = 15;

                            GCCEntities gCCEntities = new GCCEntities();
                            var subheading = "";
                            var closingRateForLongTerCapGainasJanuaryList = gCCEntities.ClosingRateForLongTerCapGainasJanuaries.ToList();
                            string[] OptionheadingsArray = Optionheadings.Split(',');
                            string[] FutureheadingsArray = Futureheadings.Split(',');
                            if (groupedposData.Any())
                            {
                                foreach (var groupedItem in groupedposData)
                                {
                                    i = 0;
                                    var foFutures = groupedItem.Select(group => group).Where(p => p.Instrument.StartsWith("FUT"));
                                    var foOptions = groupedItem.Select(group => group).Where(p => p.Instrument.StartsWith("OPT"));
                                    subheading = (groupedItem.Key.ToLower() == "eq") ? "Equity" : (groupedItem.Key.ToLower() == "cur") ? "Currency" : "Commodity";
                                    ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, i + 2, subheading + "(Options)");
                                    ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, i + 2, false);

                                    lastRow++;
                                    i = 0;
                                    for (i = 0; i < OptionheadingsArray.Length; i++)
                                    {
                                        ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, i + 2, OptionheadingsArray[i]);
                                        ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, i + 2, true);
                                    }

                                    lastRow++;
                                    i = 1;

                                    foreach (var option in foOptions)
                                    {
                                        ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, ++i, option.Symbol);
                                        ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, ++i, option.Contract);
                                        ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, ++i, option.Strikeprice);
                                        ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, ++i, option.OptionType);
                                        ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, ++i, option.TradeDate.Date);
                                        ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, ++i, option.OpenQty);
                                        ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, ++i, option.AvgRate);
                                        ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, ++i, option.AvgRate * option.OpenQty);
                                        ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, ++i, option.ClosingRate);
                                        i = 1;
                                        lastRow++;
                                    }

                                    i = 1;
                                    ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, ++i, foOptions.Sum(p => p.AvgRate * p.OpenQty));
                                    ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, i, true, "grey");
                                    ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, ++i, true, "grey");




                                    lastRow += 3;
                                    i = 2;
                                    ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow++, i, subheading + "(Futures)");
                                    ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow - 1, i, true);
                                    for (i = 0; i < FutureheadingsArray.Length; i++)
                                    {
                                        ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, i + 2, FutureheadingsArray[i]);
                                        ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, i + 2, true);
                                    }

                                    lastRow++;
                                    i = 1;
                                    foreach (var futures in foFutures)
                                    {
                                        ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, ++i, futures.Symbol);
                                        ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, ++i, futures.Contract);
                                        ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, ++i, futures.TradeDate.Date);
                                        ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, ++i, futures.OpenQty);
                                        ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, ++i, futures.AvgRate);
                                        ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, ++i, futures.AvgRate * futures.OpenQty);
                                        ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, ++i, futures.ClosingRate);
                                        i = 1;
                                        lastRow++;
                                    }
                                    ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, ++i, true, "grey");
                                    ExcelUtils.SetValueToCell(xlClosingPosWorkSheet, lastRow, ++i, foFutures.Sum(p => p.AvgRate * p.OpenQty));
                                    ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, i, true, "grey");
                                    ExcelUtils.FormatCell(xlClosingPosWorkSheet, lastRow, ++i, true, "grey");
                                    lastRow += 3;
                                }
                            }

                        }

                    }
                    #endregion

                    #region Ledger Balance
                    var xlLBWorkSheet = wbook.Worksheets.Add("Ledger Balance");
                    {
                        using (GCCEntities gcc = new GCCEntities())
                        {
                            gcc.Database.CommandTimeout = queryTimeout;

                           
                            var balanceResult = gcc.Database.SqlQuery<OtherCharges>($"SpLedgerBalancePandL_V1 {input.ClientId},'{input.FiscYear}'").ToList();
                            ExcelUtils.InsertPicture(xlLBWorkSheet, System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location) + "\\" + ConfigurationManager.AppSettings["ImagePath"], 1, 1);
                            i = 0;
                            lastRow = 6;

                            ExcelUtils.SetValueToCell(xlLBWorkSheet, lastRow, i + 2, "Trade Code");
                            ExcelUtils.FormatCell(xlLBWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlLBWorkSheet, lastRow, i + 3, client.CURLOCATION.Trim() + client.TRADECODE.Trim());
                            ExcelUtils.FormatCell(xlLBWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlLBWorkSheet, ++lastRow, i + 2, "Client Name");
                            ExcelUtils.FormatCell(xlLBWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlLBWorkSheet, lastRow, i + 3, client.NAME);
                            ExcelUtils.FormatCell(xlLBWorkSheet, lastRow, i + 3, false);

                            ExcelUtils.SetValueToCell(xlLBWorkSheet, ++lastRow, i + 2, "PAN");
                            ExcelUtils.FormatCell(xlLBWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlLBWorkSheet, lastRow, i + 3, client.PAN_GIR);
                            ExcelUtils.FormatCell(xlLBWorkSheet, lastRow, i + 3, false);

       


                            i = 0;
                            lastRow = 10;

                            ExcelUtils.SetValueToCell(xlLBWorkSheet, lastRow, i + 2, "Ledger Balance");
                            ExcelUtils.FormatCell(xlLBWorkSheet, lastRow, i + 2, false);

                            i = 0;
                            lastRow = 12;
                            ExcelUtils.SetValueToCell(xlLBWorkSheet, lastRow, i + 2, "Opening Balance");
                            ExcelUtils.FormatCell(xlLBWorkSheet, lastRow, i + 2, false);
                            ExcelUtils.SetValueToCell(xlLBWorkSheet, lastRow+1, i + 2, "Closing Balance");
                            ExcelUtils.FormatCell(xlLBWorkSheet, lastRow, i + 2, false);

                            ExcelUtils.SetValueToCell(xlLBWorkSheet, lastRow, i + 4, balanceResult[0].Amount);
                            ExcelUtils.FormatCell(xlLBWorkSheet, lastRow, i + 4, false);
                            ExcelUtils.SetValueToCell(xlLBWorkSheet, lastRow + 1, i + 4, balanceResult[1].Amount);
                            ExcelUtils.FormatCell(xlLBWorkSheet, lastRow, i + 4, false);
                        }
                    }
                    #endregion


                    #region Quarter Summary
                    summaryLastRow += 3;
                    string[] summaryquarterHeadingsArray = { "Period", "Equity Short Term", "Equity Long Term", "Bond/NCDs", "Equity F&O", "CDS F&O", "Commodity F&O", "Total" };
                    for (int k = 0; k < summaryquarterHeadingsArray.Length; k++)
                    {
                        ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, k + 2, summaryquarterHeadingsArray[k]);
                        ExcelUtils.FormatCell(xlSummaryWorkSheet, summaryLastRow, k + 2, true);
                    }
                    string[] summaryquarters = { "April to June", "July to Sept", "Oct to Dec", "Jan to March" };
                    string[] quartersToCheck = { "Q2", "Q3", "Q4", "Q1" };
                    foreach (var quarterToCheck in quartersToCheck)
                    {
                        var result = EQShortresultArray.FirstOrDefault(item => quartersToCheck.Contains(Regex.Match(item.Quarter, @"\bQ\d\b").Value));
                        if (EQShortresultArray.FirstOrDefault(item => quarterToCheck.Contains(Regex.Match(item.Quarter, @"\bQ\d\b").Value)) == null)
                        {
                            List<MyResult> EQShortresultList = EQShortresultArray.ToList();
                            EQShortresultList.Add(new MyResult { Quarter = quarterToCheck, ProfitSum = 0 });
                            EQShortresultArray = EQShortresultList.ToArray();
                        }
                        if (EQLongresultArray.FirstOrDefault(item => quarterToCheck.Contains(Regex.Match(item.Quarter, @"\bQ\d\b").Value)) == null)
                        {
                            List<MyResult> EQLongresultList = EQLongresultArray.ToList();
                            EQLongresultList.Add(new MyResult { Quarter = quarterToCheck, ProfitSum = 0 });
                            EQLongresultArray = EQLongresultList.ToArray();
                        }
                        if (EQforesultArray.FirstOrDefault(item => quarterToCheck.Contains(Regex.Match(item.Quarter, @"\bQ\d\b").Value)) == null)
                        {
                            List<MyResult> EQforesultList = EQforesultArray.ToList();
                            EQforesultList.Add(new MyResult { Quarter = quarterToCheck, ProfitSum = 0 });
                            EQforesultArray = EQforesultList.ToArray();
                        }
                        if (EQcmresultArray.FirstOrDefault(item => quarterToCheck.Contains(Regex.Match(item.Quarter, @"\bQ\d\b").Value)) == null)
                        {
                            List<MyResult> EQcmresultList = EQcmresultArray.ToList();
                            EQcmresultList.Add(new MyResult { Quarter = quarterToCheck, ProfitSum = 0 });
                            EQcmresultArray = EQcmresultList.ToArray();
                        }
                        if (EQcdsresultArray.FirstOrDefault(item => quarterToCheck.Contains(Regex.Match(item.Quarter, @"\bQ\d\b").Value)) == null)
                        {
                            List<MyResult> EQcdsresultList = EQcdsresultArray.ToList();
                            EQcdsresultList.Add(new MyResult { Quarter = quarterToCheck, ProfitSum = 0 });
                            EQcdsresultArray = EQcdsresultList.ToArray();
                        }
                        if (EQboresultArray.FirstOrDefault(item => quarterToCheck.Contains(Regex.Match(item.Quarter, @"\bQ\d\b").Value)) == null)
                        {
                            List<MyResult> EQbosresultList = EQboresultArray.ToList();
                            EQbosresultList.Add(new MyResult { Quarter = quarterToCheck, ProfitSum = 0 });
                            EQboresultArray = EQbosresultList.ToArray();
                        }
                    }
                    var x = summaryLastRow++;
                    for (int p = 0; p < quartersToCheck.Length; p++)
                    {
                        ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, 2, summaryquarters[p]);
                        ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, 3, EQShortresultArray[p].ProfitSum);
                        ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, 4, EQLongresultArray[p].ProfitSum);
                        ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, 5, EQboresultArray[p].ProfitSum);
                        ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, 6, EQforesultArray[p].ProfitSum);
                        ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, 7, EQcdsresultArray[p].ProfitSum);
                        ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, 8, EQcmresultArray[p].ProfitSum);
                        ExcelUtils.SetValueToCell(xlSummaryWorkSheet, summaryLastRow, 9, EQboresultArray[p].ProfitSum + EQcdsresultArray[p].ProfitSum + EQcmresultArray[p].ProfitSum + EQforesultArray[p].ProfitSum + EQLongresultArray[p].ProfitSum + EQShortresultArray[p].ProfitSum);
                        summaryLastRow++;
                    }


                    #endregion



                    #region Clear Tax
                    var xlCleartaxWorkSheet = wbook.Worksheets.Add("Clear Tax");
                    {
                        using (GCCEntities gcc = new GCCEntities())
                        {
                            gcc.Database.CommandTimeout = queryTimeout;
                            DateTime currentDate = DateTime.Now;
                            int financialYearStartMonth = 4;
                            var finyear = input.FiscYear;
                            
                            start = finyear.Split('-')[0] + "-04-01";
                            end = finyear.Split('-')[1] + "-03-31";
                            var Optionheadings = "ISIN,Description of Shares sold,Number of Shares,Date of Purchase,Purchase Value,Date of Sale,Sale Price per Share,FMV per Share as on 31 Jan.2018,FMV as on 31 Jan.2018,Transfer Expenses,Net capital gain";                            
                            ExcelUtils.InsertPicture(xlCleartaxWorkSheet, System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location) + "\\" + ConfigurationManager.AppSettings["ImagePath"], 1, 1);
                            i = 0;
                            lastRow = 6;
                            string[] OptionheadingsArray = Optionheadings.Split(',');
                            for (i = 0; i < OptionheadingsArray.Length; i++)
                            {
                                ExcelUtils.SetValueToCell(xlCleartaxWorkSheet, lastRow, i + 2, OptionheadingsArray[i]);
                                ExcelUtils.FormatCell(xlCleartaxWorkSheet, lastRow, i + 2, true);
                            }
                            lastRow++;
                            i = 1;
                            var closingRateForLongTerCapGainasJanuaryList = gcc.ClosingRateForLongTerCapGainasJanuaries.ToList();
                            decimal? fairMarketBuyValue = null;
                            foreach (var item in cleartaxResult)
                            {
                                ExcelUtils.SetValueToCell(xlCleartaxWorkSheet, lastRow, ++i, item.ISIN);
                                ExcelUtils.SetValueToCell(xlCleartaxWorkSheet, lastRow, ++i, item.Description);
                                ExcelUtils.SetValueToCell(xlCleartaxWorkSheet, lastRow, ++i, item.SaleQty);
                                ExcelUtils.SetValueToCell(xlCleartaxWorkSheet, lastRow, ++i, item.TranDateBuy);
                                ExcelUtils.SetValueToCell(xlCleartaxWorkSheet, lastRow, ++i, item.BuyValue);
                                ExcelUtils.SetValueToCell(xlCleartaxWorkSheet, lastRow, ++i, item.TranDateSale);
                                ExcelUtils.SetValueToCell(xlCleartaxWorkSheet, lastRow, ++i, item.SaleValue/item.SaleQty);
                                if (item.TranDateBuy.GetValueOrDefault().Date <= new DateTime(2018, 1, 31).Date
                                            && item.TranDateSale.GetValueOrDefault().Date >= new DateTime(2018, 2, 1).Date)
                                {
                                    ExcelUtils.SetValueToCell(xlCleartaxWorkSheet, lastRow, ++i, closingRateForLongTerCapGainasJanuaryList.Any(p => p.Security.Trim().ToLower() == item.Security.Trim().ToLower()) ?
                                    closingRateForLongTerCapGainasJanuaryList.FirstOrDefault(p => p.Security.Trim().ToLower() == item.Security.Trim().ToLower()).Rate.ToString() : null);
                                    ExcelUtils.SetValueToCell(xlCleartaxWorkSheet, lastRow, ++i, closingRateForLongTerCapGainasJanuaryList.Any(p => p.Security.Trim().ToLower() == item.Security.Trim().ToLower()) ?
                                        closingRateForLongTerCapGainasJanuaryList.FirstOrDefault(p => p.Security.Trim().ToLower() == item.Security.Trim().ToLower()).Rate * item.SaleQty : null);

                                    fairMarketBuyValue = closingRateForLongTerCapGainasJanuaryList.Any(p => p.Security.Trim().ToLower() == item.Security.Trim().ToLower()) ?
                                        closingRateForLongTerCapGainasJanuaryList.FirstOrDefault(p => p.Security.Trim().ToLower() == item.Security.Trim().ToLower()).Rate * item.SaleQty : null;
                                }
                                else
                                {
                                    ExcelUtils.SetValueToCell(xlCleartaxWorkSheet, lastRow, ++i, "");
                                    ExcelUtils.SetValueToCell(xlCleartaxWorkSheet, lastRow, ++i, "");
                                }
                                ExcelUtils.SetValueToCell(xlCleartaxWorkSheet, lastRow, ++i, item.PurchaseBrokerage + item.PurchaseExchangeLevy
                                                                    + item.PurchaseServiceTax + item.PurchaseStampDuty
                                                                    + item.SaleBrokerage + item.SaleExchangeLevy +
                                                                    item.SaleServiceTax + item.SaleStampDuty);
                                ExcelUtils.SetValueToCell(xlCleartaxWorkSheet, lastRow, ++i, item.SaleValue - (item.BuyValue + item.PurchaseBrokerage + item.PurchaseExchangeLevy
                                                                    + item.PurchaseServiceTax + item.PurchaseStampDuty
                                                                    + item.SaleBrokerage + item.SaleExchangeLevy +
                                                                    item.SaleServiceTax + item.SaleStampDuty));
                                i = 1;
                                lastRow++;
                            }

                                  
                            
                        }

                    }
                    #endregion
                    #region save file

                    //string password = "";

                    //if (client.Type.Trim().ToLower() == "cl")
                    //{
                    //    password = client.PAN_GIR.Substring(0, 4) + (client.DOB.HasValue ? client.DOB.Value.Day.ToString() + client.DOB.Value.Month.ToString() : "");
                    //}
                    //else
                    //{
                    //    password = client.PAN_GIR;
                    //}
                    if (!Directory.Exists(ConfigurationManager.AppSettings["FileSavePath"]))
                    {
                        Directory.CreateDirectory(ConfigurationManager.AppSettings["FileSavePath"]);
                    }
                    if (File.Exists(ConfigurationManager.AppSettings["FileSavePath"] + fileName + ".xlsx"))
                        File.Delete(ConfigurationManager.AppSettings["FileSavePath"] + fileName + ".xlsx");
                    wbook.SaveAs(ConfigurationManager.AppSettings["FileSavePath"] + fileName + ".xlsx");
                    return fileName + ".xlsx";
                    #endregion
                }
                catch (Exception ex)
                {
                    var subject = $"Error while generating P&L (RefId: {input.RefId})";
                    var message = $"Error: {ex.ToString()}";
                    var fromEmail = ConfigurationManager.AppSettings["ErrorDetailFromEmail"];
                    var toEmail = ConfigurationManager.AppSettings["ErrorDetailToEmail"];

                    SendErrorOrStopRequestEmail(subject, message, fromEmail, toEmail);

                    _logger.LogError($"Error occured on {DateTime.Now}: {ex}");


                    return "";
                }
                finally
                {
                }
            }
        }

        private string GenerateBuyNotFoundExcel(int refId, int clientId, CLIENT client, string start, string end)
        {
            try
            {
                using (GCCEntities gcc = new GCCEntities())
                {
                    gcc.Database.CommandTimeout = queryTimeout;
                    string fileName = $"BuyNotFound_{clientId}_{refId}";
                    string folderPath = ConfigurationManager.AppSettings["FileSavePath"];
                    if (!Directory.Exists(folderPath))
                    {
                        Directory.CreateDirectory(folderPath);
                    }
                    string buyNotFoundQuery = $"exec SpTaxBuyNotFoundExcel_V1 {refId}, {clientId}";
                    gcc.Database.Connection.Open();
                    var con = (SqlConnection)gcc.Database.Connection;
                    var cmd = new SqlCommand(buyNotFoundQuery, con);

                    using (var rdr = cmd.ExecuteReader())
                    {
                        int columnCount = rdr.FieldCount;
                        int rowCounter = 11;

                        if (rdr.HasRows)
                        {
                            using (var wbook = new XLWorkbook())
                            {
                                var xlBuyNotFoundWorkSheet = wbook.Worksheets.Add("Buy Not Found");
                                //ExcelUtils.InsertPicture(xlBuyNotFoundWorkSheet, Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["ImagePath"], 0, 0);
                                var summaryLastRow = 2;
                                var summaryColumnIndex = 0;

                                ExcelUtils.SetValueToCell(xlBuyNotFoundWorkSheet, summaryLastRow, summaryColumnIndex + 2, "Trade Code");
                                ExcelUtils.FormatCell(xlBuyNotFoundWorkSheet, summaryLastRow, summaryColumnIndex + 2, false);
                                ExcelUtils.SetValueToCell(xlBuyNotFoundWorkSheet, summaryLastRow, summaryColumnIndex + 3, client.CURLOCATION.Trim() + client.TRADECODE.Trim());
                                ExcelUtils.FormatCell(xlBuyNotFoundWorkSheet, summaryLastRow, summaryColumnIndex + 3, false);

                                ExcelUtils.SetValueToCell(xlBuyNotFoundWorkSheet, ++summaryLastRow, summaryColumnIndex + 2, "Client Name");
                                ExcelUtils.FormatCell(xlBuyNotFoundWorkSheet, summaryLastRow, summaryColumnIndex + 2, false);
                                ExcelUtils.SetValueToCell(xlBuyNotFoundWorkSheet, summaryLastRow, summaryColumnIndex + 3, client.NAME);
                                ExcelUtils.FormatCell(xlBuyNotFoundWorkSheet, summaryLastRow, summaryColumnIndex + 3, false);

                                ExcelUtils.SetValueToCell(xlBuyNotFoundWorkSheet, ++summaryLastRow, summaryColumnIndex + 2, "PAN");
                                ExcelUtils.FormatCell(xlBuyNotFoundWorkSheet, summaryLastRow, summaryColumnIndex + 2, false);
                                ExcelUtils.SetValueToCell(xlBuyNotFoundWorkSheet, summaryLastRow, summaryColumnIndex + 3, client.PAN_GIR);
                                ExcelUtils.FormatCell(xlBuyNotFoundWorkSheet, summaryLastRow, summaryColumnIndex + 3, false);

                                summaryLastRow = 6;
                                ExcelUtils.SetValueToCell(xlBuyNotFoundWorkSheet, summaryLastRow, summaryColumnIndex + 2, $"Details of Sale transaction for the period {Convert.ToDateTime(start).ToString("dd-MM-yyyy")} to {Convert.ToDateTime(end).ToString("dd-MM-yyyy")} for which Buy transaction details could not be found.");
                                ExcelUtils.FormatCell(xlBuyNotFoundWorkSheet, summaryLastRow, summaryColumnIndex + 2, false);

                                summaryLastRow = 8;

                                ExcelUtils.SetValueToCell(xlBuyNotFoundWorkSheet, summaryLastRow, summaryColumnIndex + 2, "Kindly update the buy transaction details in MyGeojit to get the tax statement corrected.");
                                ExcelUtils.SetValueToCell(xlBuyNotFoundWorkSheet, summaryLastRow + 1, summaryColumnIndex + 2, "Know More");
                                xlBuyNotFoundWorkSheet.Cell(summaryLastRow + 1, summaryColumnIndex + 2).SetHyperlink(new XLHyperlink(@"https://support.geojit.com/support/solutions/articles/89000007964-why-is-a-buy-not-found-file-displayed-in-my-tax-statement-zip-file"));

                                while (rdr.Read())
                                {
                                    for (int n = 0; n < columnCount; n++)
                                    {
                                        addData(xlBuyNotFoundWorkSheet, rowCounter, n + 1, rdr[rdr.GetName(n)].ToString());
                                    }
                                    rowCounter++;
                                }


                                rowCounter += 6;

                                ExcelUtils.SetValueToCell(xlBuyNotFoundWorkSheet, rowCounter, 1, $"Created: {DateTime.Now.ToString("dd.MM.yyyy h:m:ss")}");

                                string password = "";

                                if (client.Type.Trim().ToLower() == "cl")
                                {
                                    password = client.PAN_GIR.Substring(0, 4) + (client.DOB.HasValue ? client.DOB.Value.Day.ToString() + client.DOB.Value.Month.ToString() : "");
                                }
                                else
                                {
                                    password = client.PAN_GIR;
                                }

                                if (!Directory.Exists(ConfigurationManager.AppSettings["FileSavePath"]))
                                {
                                    Directory.CreateDirectory(ConfigurationManager.AppSettings["FileSavePath"]);
                                }
                                if (File.Exists(ConfigurationManager.AppSettings["FileSavePath"] + fileName + ".xlsx"))
                                    File.Delete(ConfigurationManager.AppSettings["FileSavePath"] + fileName + ".xlsx");

                                wbook.SaveAs(ConfigurationManager.AppSettings["FileSavePath"] + fileName + ".xlsx");
                                return fileName + ".xlsx"; 
                            }
                        }
                        return string.Empty;
                    }
                }
            }
            catch (Exception ex)
            {

                var subject = $"Error while generating BuyNotFound (RefId: {refId})";
                var message = $"Error: {ex.ToString()}";
                var fromEmail = ConfigurationManager.AppSettings["ErrorDetailFromEmail"];
                var toEmail = ConfigurationManager.AppSettings["ErrorDetailToEmail"];

                SendErrorOrStopRequestEmail(subject, message, fromEmail, toEmail);

                _logger.LogError($"Error occured on {DateTime.Now}: {ex}");
                return "";
            }

            finally
            {
            }

            
        }

        private List<string> GenerateSTTFormNo10dbAutoTaxStatement(CLIENT client, string finYear)
        {
            IList<string> result = new List<string>();
            try
            {
                using (GCCEntities gcc = new GCCEntities())
                {
                    gcc.Database.CommandTimeout = queryTimeout;
                    //603076_06092022161923923FormNo10DB_BSE
                    string fileName = $"{client.TRADECODE}_{DateTime.Now.ToString("yyyyMMddhhmmssfff")}_FormNo10DB";
                    string folderPath = ConfigurationManager.AppSettings["FileSavePath"];
                    if (!Directory.Exists(folderPath))
                    {
                        Directory.CreateDirectory(folderPath);
                    }
                    string sttformNo10db_AutoTaxStatementQuery = $"exec SpSTTFormNo10db_AutoTaxStatement {client.CLIENTID},'','{finYear}'";
                    var sttformNo10db_AutoTaxStatementData = gcc.Database.SqlQuery<_10DBStatementModel>(sttformNo10db_AutoTaxStatementQuery).ToList();

                    var bseStatements = sttformNo10db_AutoTaxStatementData.Where(p => p.product.Trim().ToLower() == "bse").ToList();
                    var nseStatements = sttformNo10db_AutoTaxStatementData.Where(p => p.product.Trim().ToLower() == "nse").ToList();

                    if (bseStatements.Any() && bseStatements.Sum(p => p.STT) != 0)
                    {
                        var pdfGenerationStatus = PdfGenerator.GeneratePdf(bseStatements, folderPath + fileName + "_BSE.pdf");
                        //if (pdfGenerationStatus)
                        if (File.Exists(folderPath + fileName + "_BSE.pdf"))
                            result.Add(fileName + "_BSE.pdf");
                        else
                            _logger.LogError($"Error occured on {DateTime.Now}: BSE statement couldn't generated");
                    }

                    if (nseStatements.Any() && nseStatements.Sum(p => p.STT) != 0)
                    {
                        var pdfGenerationStatus = PdfGenerator.GeneratePdf(nseStatements, folderPath + fileName + "_NSE.pdf");
                        //if (pdfGenerationStatus)
                        if (File.Exists(folderPath + fileName + "_NSE.pdf"))
                            result.Add(fileName + "_NSE.pdf");
                        else
                            _logger.LogError($"Error occured on {DateTime.Now}: NSE statement couldn't generated");
                    }
                }
                return result.ToList();
            }
            catch (Exception ex)
            {

                var subject = $"Error while generating STTFormNo10dbAutoTaxStatement ClientId: {client.CLIENTID} (Code: {client.CURLOCATION.Trim() + client.TRADECODE.Trim()})";
                var message = $"Error: {ex.ToString()}";
                var fromEmail = ConfigurationManager.AppSettings["ErrorDetailFromEmail"];
                var toEmail = ConfigurationManager.AppSettings["ErrorDetailToEmail"];

                SendErrorOrStopRequestEmail(subject, message, fromEmail, toEmail);

                _logger.LogError($"Error occured on {DateTime.Now}: {ex}");
                return null;
            }

            finally
            {
            }
        }


        #region Helpers
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    _logger.LogInformation("Process started");
                    lock (_object)
                    {
                        Run();
                    }
                    _logger.LogInformation("Process completed");

                    //await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
                    await Task.Delay(TimeSpan.FromSeconds(Convert.ToInt32(ConfigurationManager.AppSettings["ProcessInterval"])), stoppingToken);
                }
            }
            catch (Exception ex)
            {

                var subject = "Error in P&L Service";
                var message = $"Error: {ex}";
                var fromEmail = ConfigurationManager.AppSettings["ErrorDetailFromEmail"];
                var toEmail = ConfigurationManager.AppSettings["ErrorDetailToEmail"];

                SendErrorOrStopRequestEmail(subject, message, fromEmail, toEmail);

                _logger.LogError(ex.Message);

                // Terminates this process and returns an exit code to the operating system.
                // This is required to avoid the 'BackgroundServiceExceptionBehavior', which
                // performs one of two scenarios:
                // 1. When set to "Ignore": will do nothing at all, errors cause zombie services.
                // 2. When set to "StopHost": will cleanly stop the host, and log errors.
                //
                // In order for the Windows Service Management system to leverage configured
                // recovery options, we need to terminate the process with a non-zero exit code.
                Environment.Exit(1);
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            var subject = "P&L Service is stopping";
            var message = "Stop request fired, service is stopping execution";
            var fromEmail = ConfigurationManager.AppSettings["ErrorDetailFromEmail"];
            var toEmail = ConfigurationManager.AppSettings["ErrorDetailToEmail"];

            SendErrorOrStopRequestEmail(subject, message, fromEmail, toEmail);
            await base.StopAsync(cancellationToken);
        }
        void addData(IXLWorksheet sheet, int row, int col, string data)
        {
            if (!String.IsNullOrEmpty(data) && data.Trim().ToUpper() == "CLIENT CODE")
            {
                data = "Trade Code";
            }

            if (!String.IsNullOrEmpty(data) && data.Trim().ToUpper() == "TRANDATE")
            {
                data = "Sell Transaction date";
            }

            ExcelUtils.SetValueToCell(sheet, row, col, data);
        }
        private string MakeZip(List<string> fileNames, string password, CLIENT client)
        {
            var file = "";

            using (Archive archive = new Archive(new ArchiveEntrySettings(encryptionSettings: new TraditionalEncryptionSettings(password))))
            {
                foreach (var fileName in fileNames.Where(x => !string.IsNullOrEmpty(x)))
                {
                    archive.CreateEntry(fileName, ConfigurationManager.AppSettings["FileSavePath"] + fileName);
                }

                file = $"Profit & Loss_{client.TRADECODE}_{DateTime.Now.ToString("yyyyMMddhhmmssfff")}.zip";
                archive.Save(ConfigurationManager.AppSettings["FileSavePath"] + file);
            }
            return file;
        }
        private bool SendEmailEmailToClientWithAttachment(string fileName, string toEmail, string ccEmail, string name, string emailSubject)
        {
            toEmail = "akshara.shylajan@simelabs.com";
            ccEmail = "akshara.shylajan@simelabs.com";
            try
            {
            using (SmtpClient SmtpServer = new SmtpClient(ConfigurationManager.AppSettings["Host"]))
                {
                    SmtpServer.UseDefaultCredentials = true;

                    SmtpServer.Port = Convert.ToInt32(ConfigurationManager.AppSettings["SMTPPort"]);
                    SmtpServer.Credentials = new System.Net.NetworkCredential(ConfigurationManager.AppSettings["SMTPUserName"], ConfigurationManager.AppSettings["SMTPPassword"]);
                    var emailTemplate = File.ReadAllText(Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location) + "\\" + ConfigurationManager.AppSettings["EmailTemplatePath"]);
                    emailTemplate = emailTemplate.Replace("{name}", name);
                    using (MailMessage mail = new MailMessage())
                    {
                        mail.From = new MailAddress(ConfigurationManager.AppSettings["FromEmail"]);
                        mail.To.Add(toEmail);
                        mail.CC.Add(ccEmail);
                        mail.Subject = emailSubject;
                        mail.Body = emailTemplate;
                        mail.BodyEncoding = Encoding.UTF8;
                        mail.IsBodyHtml = true;                        

                        Attachment attachment;
                        attachment = new Attachment(ConfigurationManager.AppSettings["FileSavePath"] + fileName);
                        mail.Attachments.Add(attachment);

                        SmtpServer.Send(mail);
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error occured on {DateTime.Now}, while sending P&L file for the refId:{pnLInputModel_for_log.RefId}, Error:\n {ex}");
                throw ex;
            }
        }

        public void ExportToPdf(DataTable dt, string strFilePath)
        {
            iTextSharp.text.Document document = new iTextSharp.text.Document();
            PdfWriter writer = PdfWriter.GetInstance(document, new FileStream(strFilePath, FileMode.Create));
            document.Open();
            iTextSharp.text.Font font5 = iTextSharp.text.FontFactory.GetFont(FontFactory.HELVETICA, 5);

            PdfPTable table = new PdfPTable(dt.Columns.Count);
            float[] widths = new float[dt.Columns.Count];
            for (int i = 0; i < dt.Columns.Count; i++)
                widths[i] = 4f;

            table.SetWidths(widths);

            table.WidthPercentage = 100;
            PdfPCell cell = new PdfPCell(new Phrase("Products"));

            cell.Colspan = dt.Columns.Count;

            foreach (DataColumn c in dt.Columns)
            {
                table.AddCell(new Phrase(c.ColumnName, font5));
            }

            foreach (DataRow r in dt.Rows)
            {
                if (dt.Rows.Count > 0)
                {
                    for (int h = 0; h < dt.Columns.Count; h++)
                    {
                        table.AddCell(new Phrase(r[h].ToString(), font5));
                    }
                }
            }
            document.Add(table);
            document.Close();
        }
        private static string GetQuarter(DateTime date)
        {
            int quarter = (date.Month - 1) / 3 + 1;
            return $"{date.Year} Q{quarter}";
        }
        public static string GetLocalIPAddress()
        {
            var host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (var ip in host.AddressList)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    return ip.ToString();
                }
            }
            return null;
        }


        public void SendErrorOrStopRequestEmail(string emailSubject, string message, string fromEmail, string toEmail)
        {
            toEmail = "akshara.shylajan@simelabs.com";

            try
            {
                using (SmtpClient SmtpServer = new SmtpClient(ConfigurationManager.AppSettings["Host"]))
                {
                    SmtpServer.UseDefaultCredentials = true;

                    SmtpServer.Port = Convert.ToInt32(ConfigurationManager.AppSettings["SMTPPort"]);
                    SmtpServer.Credentials = new System.Net.NetworkCredential(ConfigurationManager.AppSettings["SMTPUserName"], ConfigurationManager.AppSettings["SMTPPassword"]);

                    using (MailMessage mail = new MailMessage())
                    {
                        mail.From = new MailAddress(fromEmail);
                        mail.To.Add(toEmail);
                        mail.Subject = emailSubject;
                        mail.Body = message;

                        SmtpServer.Send(mail);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error occured on {DateTime.Now}: {ex}");
            }
        }
        #endregion
    }
}