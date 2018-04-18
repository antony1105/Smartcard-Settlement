create or replace package
PKG_FSS_SETTLEMENT AS
--
-- Package:  Pkg_FSS_Settlement 
-- Author:   Kazuki Yamada
-- Date:     17-May-2016
/*
-- Description
   This package will implement the Assignment specification for Database Programming
   The package contains code that will settle the transactions received from
   terminals at merchant locations. The transactions that are settled will be marked
   as settle by the use of a Lodgement reference number.
*/
-- Modification History 
-- 17-MAY-2016: Package created
-- 27-MAY-2016: Package compiling complete

 PROCEDURE DailySettlement; 
 PROCEDURE DailyBankingSummary(p_report_date DATE default sysdate); 
 PROCEDURE FraudReport;
 
END PKG_FSS_SETTLEMENT;