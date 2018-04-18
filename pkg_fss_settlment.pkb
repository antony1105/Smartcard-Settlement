create or replace PACKAGE BODY
PKG_FSS_SETTLEMENT AS

  -- Package:  Pkg_FSS_Settlement 
  -- Author:   Kazuki Yamada
  -- Date:     11/MAY/2016 
  /* 
  -- Description
     This package will implement the Assignment specification for 
     Database Programming. The package contains code that will settle
     the transactions received from terminals at merchant locations. 
     The transactions that are settled will be marked as settle by the
     use of a Lodgement reference number. 
  */

  -- GLOBAL VARIABLES
  v_run_ok BOOLEAN;

  --Forward Declerations,
  PROCEDURE Announceme(p_module_name VARCHAR2,
                     p_destination   VARCHAR2 default 'T');
  PROCEDURE run_failed(p_remarks IN VARCHAR2 DEFAULT 'Run failed');
  PROCEDURE Check_run_table(p_runid    IN OUT NUMBER, 
                            p_run_data IN OUT FSS_RUN_TABLE%ROWTYPE);
  PROCEDURE upload_new_Transactions;
  PROCEDURE settle_transactions;
  PROCEDURE upload_lodgementnr(p_merchantid   IN NUMBER, 
                               p_lodgementnum IN VARCHAR2);
  PROCEDURE create_deskbank(p_record_date IN DATE DEFAULT SYSDATE);
  PROCEDURE debit_transaction(p_debit IN NUMBER);  
  PROCEDURE email_summary(p_reportdate IN DATE DEFAULT sysdate);  
  PROCEDURE create_fraud_report;  
  
  FUNCTION get_email_recipients(mail_conn IN OUT UTL_SMTP.connection) RETURN NUMBER;
  FUNCTION get_lodgement_num RETURN VARCHAR2;
  FUNCTION format_currency(p_currency IN NUMBER) RETURN VARCHAR2;
  
      
  /* PRIVATE PROCEDURES AND FUNCIONS */
  PROCEDURE Announceme(p_module_name VARCHAR2,
                       p_destination VARCHAR2 DEFAULT 'T') IS
  --
  -- This module prints out the message that I want to print
  -- By default it will print into the DBP_MESSAGE_LOG table
  -- but if I pass the destination parameter as S then the 
  -- printout will be on the screen. Make sure you issue SET SERVEROUTPUT ON
  --
    v_message VARCHAR2(255) := 'In module '||p_module_name;
  BEGIN
    IF p_destination = 'T' THEN
      common.log(v_message);
    ELSE
      DBMS_OUTPUT.put_line(v_message);
    END IF;
  END;
  
   PROCEDURE run_failed(p_remarks IN VARCHAR2 DEFAULT 'Run failed') IS
  --Purpose: Procedure logs if the run has failed
  --Author:  Kazuki Yamada
  --Date:    25/MAY/2016 
    v_runLogID NUMBER;    
    v_module_name  VARCHAR2(35) := 'run_failed';
  BEGIN
    Announceme(v_module_name||' '||p_remarks);
    SELECT RUNID INTO v_runLogID FROM FSS_RUN_TABLE
     WHERE TRUNC(RUNSTART) = TRUNC(sysdate);
     
     UPDATE FSS_RUN_TABLE
        SET RUNEND      = sysdate,
            RUNOUTCOME  = 'FAILED',
            REMARKS     = p_remarks
      WHERE RUNID = v_runLogID;
    v_run_ok := FALSE;
  END;
  
  PROCEDURE Check_run_table(p_runid    IN OUT NUMBER,
                            p_run_data IN OUT FSS_RUN_TABLE%ROWTYPE) IS
    --Purpose: Procedure checks the run table if the program has been run
    --Author:  Kazuki Yamada
    --Date:    12/MAY/2016
  v_module_name  VARCHAR2(35) := 'Check_run_table';
  v_runLogID     NUMBER;
  moduleRan      EXCEPTION;
  BEGIN
    Announceme(v_module_name||'. Checking run table if program was already run today');
    BEGIN
      SELECT * INTO p_run_data FROM FSS_RUN_TABLE
              WHERE RUNOUTCOME = 'SUCCESS'
                AND TRUNC(RUNEND) = TRUNC(sysdate);
              RAISE moduleRan; -- If value was found, exception will be raised
               
      -- If program was not run
      EXCEPTION WHEN NO_DATA_FOUND THEN
        p_runid := FSS_RUNLOG_SEQ.NEXTVAL;                       
        INSERT INTO FSS_RUN_TABLE(RUNID, RUNSTART, RUNEND, RUNOUTCOME, REMARKS)
        VALUES (p_runid, sysdate, NULL, NULL, 'Start Program');
    END;
    
    EXCEPTION WHEN moduleRan THEN
      p_runid := -1;
      Announceme('Check the FSS_RUN_TABLE. Program ran at '|| p_run_data.RUNSTART);
    WHEN OTHERS THEN      
      run_failed(TO_CHAR('Error occured in '||v_module_name||' with '||SQLERRM));
  END; 
    
  PROCEDURE upload_new_Transactions IS
  --Purpose: Upload any new transactions from the main table into mine  
  --Author:  Kazuki Yamada
  --Date:    11/MAY/2016
  v_module_name VARCHAR2(35) := 'upload_new_Transactions';
  BEGIN
    Announceme(v_module_name||'. Finding any new transactions and inserting them into FSS_DAILY_TRANSACTION');
    INSERT INTO FSS_DAILY_TRANSACTION(TRANSACTIONNR,
                                    DOWNLOADDATE,
                                    TERMINALID,
                                    CARDID,
                                    TRANSACTIONDATE,
                                    CARDOLDVALUE,
                                    TRANSACTIONAMOUNT,
                                    CARDNEWVALUE,
                                    TRANSACTIONSTATUS,
                                    ERRORCODE)
         SELECT t1.TRANSACTIONNR,
                t1.DOWNLOADDATE,
                t1.TERMINALID,
                t1.CARDID,
                t1.TRANSACTIONDATE,
                t1.CARDOLDVALUE,
                t1.TRANSACTIONAMOUNT,
                t1.CARDNEWVALUE,
                t1.TRANSACTIONSTATUS,
                t1.ERRORCODE 
           FROM fss_transactions t1
    WHERE NOT EXISTS (SELECT 1
                      FROM FSS_DAILY_TRANSACTION t2                  
                      WHERE t1.transactionnr = t2.transactionnr);
    COMMIT; 
  EXCEPTION 
    WHEN OTHERS THEN      
       run_failed(TO_CHAR('Error occured in '||v_module_name||' with '||SQLERRM));
  END;    
  
  PROCEDURE settle_transactions IS
  --Purpose: This procedure will settle the transactions of the smartcards by
  --         taking unsettled transactions and add them to FSS_DAILY_SETTLEMENT
  --         to settle, and also generates a deskbank file
  --Author:  Kazuki Yamada
  --Date:    11/MAY/2016
  v_credit            NUMBER       :=0;
  v_nrRecords         NUMBER       :=0;
  v_module_name       VARCHAR2(35) :='settle_transactions';
  v_lodgementnum      VARCHAR2(15);
  no_settlements  EXCEPTION;
  
  v_minimum_settle NUMBER;
  v_file utl_file.file_type;
  CURSOR c_merchant_totals IS
         SELECT m.MERCHANTBANKBSB bsb, 
                m.MERCHANTBANKACCNR act,
                SUBSTR(m.MERCHANTLASTNAME, 1, 32) Name, --Deskbank file only accepts 32 chars so note the SUBSTR
                M.MERCHANTID merchantid,
                SUM(t.TRANSACTIONAMOUNT) total
           FROM fss_merchant m JOIN fss_terminal term ON m.MERCHANTID = term.MERCHANTID 
           JOIN FSS_DAILY_TRANSACTION t ON term.TERMINALID = t.TERMINALID 
          WHERE LODGEMENTNR IS null  --Settled transactions have the lod gementnr stamped pick up only those that have to be settled
          GROUP BY m.MERCHANTLASTNAME, M.MERCHANTID, m.MERCHANTBANKBSB, m.MERCHANTBANKACCNR;

  r_merchant_total c_merchant_totals%ROWTYPE;
  
  BEGIN
    Announceme(v_module_name||'. Finding any unsettled transactions and inserting them into FSS_DAILY_SETTLEMENT');
    -- Strip decimal and get daily minimum value for settlement
    SELECT TO_NUMBER(REPLACE(REFERENCEVALUE,'.')) INTO v_minimum_settle
    FROM FSS_REFERENCE WHERE REFERENCEID='DMIN';        
    
    -- Loop through all unsettled accounts
    FOR r_merchant_total IN c_merchant_totals LOOP
      IF r_merchant_total.total > v_minimum_settle THEN    
        -- Generate lodgement number
        
        -- Keep track of total settlement amount and count 
        v_credit := v_credit + r_merchant_total.total;
        v_nrRecords := v_nrRecords + 1;
        v_lodgementnum := get_lodgement_num;
        
        INSERT INTO FSS_DAILY_SETTLEMENT (LODGEMENTNR, 
                                          RECORDTYPE,
                                          BSB,
                                          ACCOUNTNR,
                                          TRAN_CODE,
                                          SETTLEVALUE,
                                          MERCHANTID,
                                          MERCHANTTITLE, 
                                          BANKFLAG,
                                          TRACE,
                                          REMITTER,
                                          GSTTAX,
                                          SETTLEDATE)
                                  VALUES (v_lodgementnum, 
                                          1,
                                          r_merchant_total.bsb,
                                          r_merchant_total.act,
                                          50, 
                                          r_merchant_total.total,
                                          r_merchant_total.merchantid,
                                          r_merchant_total.Name, 
                                          'F',
                                          '032-797 001006', 
                                          'SMARTCARD TRANS',
                                          '00000000', 
                                          sysdate);
        -- update lodgement number for merchant's transactions
        upload_lodgementnr(r_merchant_total.merchantid, v_lodgementnum);
      END IF;
      EXIT WHEN v_nrRecords > 50;
    END LOOP;
        
    -- Calculate the amount to debit
    IF v_nrRecords > 0 THEN     
      v_nrRecords := v_nrRecords + 1;
      
      debit_transaction(v_credit);
      COMMIT;
      -- Generate deskbank file
      create_deskbank;
      -- Generate deskbank summary
      DailyBankingSummary;    
    ELSE
      -- If the counter is 0 then raise exception
      RAISE no_settlements;
    END IF;     
    EXCEPTION
      WHEN no_settlements THEN
        common.log('No unsettled transactions found on '||to_char(sysdate));
      WHEN OTHERS THEN
        run_failed(TO_CHAR('Error occured in '||v_module_name||' with '||SQLERRM));
  END;
  
  PROCEDURE upload_lodgementnr(p_merchantid   IN NUMBER, 
                              p_lodgementnum IN VARCHAR2) IS  
  --Purpose: Procedure uploads lodgement number to the relavent transactions
  --         in the FSS_DAILY_TRANSACTION table
  --Author:  Kazuki Yamada
  --Date:    19/MAY/2016
  v_module_name  VARCHAR2(35) := 'upload_lodgementnr';
  BEGIN
    Announceme(v_module_name||'. Uploading lodgement number '||p_lodgementnum||' into FSS_DAILY_TRANSACTION for merchant '||p_merchantid);    
    UPDATE FSS_DAILY_TRANSACTION trn 
       SET trn.LODGEMENTNR = p_lodgementnum
     WHERE EXISTS (SELECT p_merchantid 
                     FROM FSS_TERMINAL ter
                    WHERE trn.TERMINALID=ter.TERMINALID
                      AND ter.MERCHANTID=p_merchantid
                      AND trn.LODGEMENTNR IS NULL);  
                      
    EXCEPTION
      WHEN OTHERS THEN
        run_failed(TO_CHAR('Error occured in '||v_module_name||' with '||SQLERRM));
  END;
      
  PROCEDURE create_deskbank(p_record_date IN DATE DEFAULT SYSDATE) IS
  --Purpose: This procedure generates a deskbank file 
  --Author:  Kazuki Yamada
  --Date:    25/MAY/2016
    v_module_name       VARCHAR2(35) := 'create_deskbank';
    v_deskbank_filename VARCHAR2(30) := 'DS_'||to_char(sysdate,'DDMMYYYY')||'_KY.dat';
    v_fileline          VARCHAR2(150);
    v_orgtitle          VARCHAR2(20);
    v_orgbsb            VARCHAR2(20);
    v_credit            NUMBER :=0;
    v_nrRecords         NUMBER :=0;
    v_debit             NUMBER :=0;
    v_file utl_file.file_type;    
    
    CURSOR c_settlements IS
      SELECT LODGEMENTNR,
             TRAN_CODE,
             ACCOUNTNR,
             BSB,
             SETTLEVALUE,
             MERCHANTTITLE,
             BANKFLAG
        FROM FSS_DAILY_SETTLEMENT
       WHERE TRUNC(SETTLEDATE)=TRUNC(p_record_date);
  BEGIN
    Announceme(v_module_name||'. Generating deskbank report for '||to_char(p_record_date, 'DD-MM-YYYY'));
    
    -- Print header to file
    SELECT ORGBSBNR, ORGACCOUNTTITLE INTO v_orgtitle, v_orgbsb 
    FROM FSS_ORGANISATION;
    
    v_file := utl_file.fopen ('KAYAMADA_DIR',v_deskbank_filename,'W');    
    
    v_fileline := to_char(RPAD('0', 18) || 
                          RPAD('01WBC', 12) || 
                          RPAD(v_orgtitle, 26, ' ') || 
                          v_orgbsb || 
                          RPAD('INVOICES', 12, ' ') ||
                          to_char(sysdate, 'DDMMYY'));
                          
    utl_file.put_line(v_file, v_fileline); 
    
    -- Print 
    FOR r_settlements IN c_settlements LOOP
      v_nrRecords := v_nrRecords + 1;
      v_fileline := TO_CHAR(1 ||
                            SUBSTR(r_settlements.bsb,1,3)||
                            '-'|| 
                            SUBSTR(r_settlements.bsb,4,3)||
                            r_settlements.ACCOUNTNR|| 
                            ' '||
                            RPAD(r_settlements.TRAN_CODE, 2, '0')||
                            LPAD(r_settlements.settlevalue,10,'0')||
                            RPAD(r_settlements.MERCHANTTITLE,32,' ')||
                            ' '||r_settlements.BANKFLAG||' '||
                            r_settlements.lodgementnr||
                            '032-797 001006SMARTCARD TRANS '||
                            RPAD('0', 8, '0'));
                            
       IF r_settlements.TRAN_CODE = 50 THEN
         v_credit := v_credit + r_settlements.settlevalue;
       ELSE   
         v_debit := v_debit + r_settlements.settlevalue;
       END IF;
       
       utl_file.put_line(v_file, v_fileline);
       
       EXIT WHEN v_nrRecords > 100;
    END LOOP;
    
    -- Print footer
    v_fileline := TO_CHAR(RPAD('7999-999',20, ' ') ||
                          RPAD(v_debit-v_credit,10,'0') ||
                          LPAD(v_credit,10,'0') ||
                          LPAD(v_debit,10,'0') ||
                          LPAD(' ',24) ||
                          LPAD(v_nrRecords,6,'0'));
    utl_file.put_line(v_file, v_fileline);
        
    utl_file.fclose(v_file);
    
    EXCEPTION
      WHEN OTHERS THEN
        utl_file.fclose(v_file);
        run_failed(TO_CHAR('Error occured in '||v_module_name||' with '||SQLERRM));
  END;    
    
  
  PROCEDURE email_summary (p_reportdate IN DATE DEFAULT sysdate) IS
  --Purpose: Procedure emails the daily settlement summary to selected 
  --         recipients in the the PARAMETER TABLE.
  --Author:  Kazuki Yamada
  --Date:    23/MAY/2016
  v_module_name    VARCHAR2(50) := 'email_summary';
  
  mail_conn        UTL_SMTP.connection;
  p_subject        VARCHAR2(50) := 'Deskbank summary report for '||to_char(p_reportdate, 'DD-Mon-YYYY');
  p_message        VARCHAR2(120) := 'Attached is the deskbank summary report for '||to_char(p_reportdate, 'DD-Mon-YYYY');
  v_sender         VARCHAR2(50) := 'sender@example.com';
  v_mailhost       VARCHAR2(50) := 'host.example.com';
  
  v_boundary_text  VARCHAR2(25) := 'Boundary text';
  v_counter        NUMBER := 0;
  con_nl           VARCHAR2(2) := CHR(13)||CHR(10); -- Newline
  con_email_footer VARCHAR2(250) := 'This is the email footer';
  v_attachment VARCHAR2(80) := 'DailyBankingSummary'||TO_CHAR(p_reportdate, 'DD-Mon-YYYY')||'_KY.txt';
  v_file utl_file.file_type;  
  
  CURSOR c_recipients IS
    SELECT VALUE FROM PARAMETER 
     WHERE KIND='EMAIL_ADDRESS'
       AND CODE='ASSGN_RECIPIENT'
       AND ACTIVE='Y';
         
  r_recipients c_recipients%rowtype;
  NO_RECIPIENTS EXCEPTION;
  
  BEGIN
    Announceme(v_module_name||'. Emailing out deskbank summaries');
  
    -- Open connection
    mail_conn := UTL_SMTP.open_connection(v_mailhost, 25);
    UTL_SMTP.helo (mail_conn, v_mailhost);
    UTL_SMTP.mail (mail_conn, v_sender);
    
    v_file := UTL_FILE.FOPEN('KAYAMADA_DIR',v_attachment,'R');
    
    -- Send deskbank summary reports to 
    IF get_email_recipients(mail_conn) = 0 THEN
      RAISE NO_RECIPIENTS;
    END IF;
    
    v_attachment := REPLACE(v_attachment, '_KY');
    UTL_SMTP.open_data  (mail_conn);
    UTL_SMTP.write_data (mail_conn,'From' || ':' || v_sender|| con_nl);
    UTL_SMTP.write_data (mail_conn,'To'|| ':'|| r_recipients.VALUE|| con_nl);
    UTL_SMTP.write_data (mail_conn,'Subject'|| ':'|| p_subject||con_nl);
    
    UTL_SMTP.write_data (mail_conn,'Mime-Version: 1.0'||con_nl);
    UTL_SMTP.write_data (mail_conn,'Content-Type: multipart/mixed; boundary="'||v_boundary_text||'"'||con_nl);
    UTL_SMTP.write_data (mail_conn,'--'||v_boundary_text||con_nl);
    UTL_SMTP.write_data (mail_conn,'Content-type: text/plain; charset=us-ascii'||con_nl);

    --Email body
    UTL_SMTP.write_data (mail_conn, con_nl || p_message||con_nl);
    UTL_SMTP.write_data (mail_conn,con_nl||'--'||v_boundary_text||con_nl);
    UTL_SMTP.write_data (mail_conn,'Content-Type: application/octet-stream; name="'||v_attachment||'"'||con_nl);
    UTL_SMTP.write_data (mail_conn,'Content-Transfer-Encoding: 7bit'||con_nl||con_nl);     
      
    -- Write attachment
    IF UTL_FILE.IS_OPEN(v_file) THEN
    v_counter := 0;
      LOOP
        BEGIN
        v_counter := v_counter + 1;
        UTL_FILE.GET_LINE(v_file, p_message);
        UTL_SMTP.write_data (mail_conn, p_message||con_nl);
        EXIT WHEN v_counter > 100;
        EXCEPTION 
          WHEN No_Data_Found THEN EXIT; END;
      END LOOP;
    END IF;
    UTL_SMTP.write_data (mail_conn,con_nl||'--'||v_boundary_text||'--'||con_nl);
      
    --Email Footer
    UTL_SMTP.write_data (mail_conn, con_nl || con_email_footer||con_nl);
    UTL_SMTP.close_data (mail_conn);
    UTL_SMTP.quit (mail_conn);
      
    utl_file.fclose(v_file);     
    
  EXCEPTION
    WHEN NO_RECIPIENTS THEN
      common.log('No recipients to email Deskbank Summary to');
      UTL_SMTP.close_data (mail_conn);
    WHEN OTHERS THEN
      run_failed(TO_CHAR('Error occured in '||v_module_name||' with '||SQLERRM));
      UTL_SMTP.close_data (mail_conn);
  END;
  
  
  PROCEDURE create_fraud_report IS
  --Purpose: Procedure writes report of suspicious transactions to file
  --Author:  Kazuki Yamada
  --Date:    25/MAY/2016
    CURSOR c_fraud IS 
      SELECT TRANSACTIONNR,
             PREVTRNNR,
             CARDID,
             TRANSACTIONDATE,
             COMMENTS
        FROM FSS_FRAUD;
    v_file utl_file.file_type;
    v_filename    VARCHAR2(40) := 'FraudReport_'||to_char(sysdate,'DDMMYYYY')||'_KY.txt';
    v_module_name VARCHAR2(20) := 'create_fraud_report';
    v_fileline    VARCHAR2(255);
    v_counter     NUMBER := 0;
    
  BEGIN
    announceme(v_module_name ||'. Generating report for suspected transactions');
    v_file := utl_file.fopen ('KAYAMADA_DIR',v_filename,'W');
    
    -- Write fraud report heading
    v_fileline := LPAD(' ', 30, ' ') || 'SMARTCARD SETTLEMENT SYSTEM';
    utl_file.put_line(v_file, v_fileline);    
    v_fileline := LPAD(' ', 32, ' ') || 'FRAUD REPORT';
    utl_file.put_line(v_file, v_fileline);
    
    v_fileline := TO_CHAR('Date '||
                          TO_CHAR(sysdate,'DD-MM-YYYY'));    
    utl_file.put_line(v_file, v_fileline);
    
    utl_file.put_line(v_file, '');    
    v_fileline := RPAD('TRANSACTION NUMBER', 19, ' ')    ||
                  RPAD('CARDID', 18, ' ')  ||                  
                  RPAD('TRANSACTION DATE', 20, ' ');
    utl_file.put_line(v_file, v_fileline);   
    
    v_fileline := RPAD('-', 18, '-')||' '||
                  RPAD('-', 17, '-')||' '||
                  RPAD('-', 19, '-');   
    utl_file.put_line(v_file, v_fileline);
    
    -- Write body
    FOR r_fraud IN c_fraud LOOP
      v_counter := v_counter+1;
      v_fileline := RPAD(r_fraud.TRANSACTIONNR, 19, ' ')||
                    RPAD(r_fraud.CARDID, 18, ' ')||                  
                    RPAD(TO_CHAR(r_fraud.TRANSACTIONDATE, 'DD-MON-YYYY HH24:MI'), 20, ' ');
      utl_file.put_line(v_file, v_fileline);
      
      v_fileline:= 'REMARKS: '||SUBSTR(r_fraud.COMMENTS, 0, 120);
      utl_file.put_line(v_file, v_fileline);
      
      IF LENGTH(r_fraud.COMMENTS) > 120 THEN
        v_fileline := SUBSTR(r_fraud.COMMENTS, 121, 255);
        utl_file.put_line(v_file, v_fileline);
      END IF;
      
      utl_file.put_line(v_file, '');
      
    END LOOP;
    utl_file.fclose(v_file);
    
    EXCEPTION 
      WHEN OTHERS THEN
        utl_file.fclose(v_file);    
  END;
  
  PROCEDURE debit_transaction(p_debit IN NUMBER) IS
  --Purpose: Function uploads debit transaction
  --Author:  Kazuki Yamada
  --Date:    18/MAY/2016                             

    v_module_name  VARCHAR2(35) := 'debit_transaction';
    v_lodgementnum VARCHAR2(15);
    v_account_no   VARCHAR2(10);
    v_title        VARCHAR2(32);
    v_bsb          VARCHAR2(6);    
  BEGIN
    Announceme(v_module_name||'. Calculating amount needed to debit, and insert into FSS_DAILY_SETTLEMENT');   
    v_lodgementnum := get_lodgement_num;
    
    SELECT ORGACCOUNTTITLE,
           ORGBSBNR,
           ORGBANKACCOUNT
      INTO v_title,v_bsb,v_account_no
      FROM FSS_ORGANISATION
     WHERE ROWNUM<2;      
    INSERT INTO FSS_DAILY_SETTLEMENT (RECORDTYPE, 
                                      BSB, 
                                      ACCOUNTNR,
                                      TRAN_CODE,
                                      SETTLEVALUE,
                                      MERCHANTTITLE, 
                                      BANKFLAG,
                                      LODGEMENTNR, 
                                      TRACE, 
                                      REMITTER,
                                      GSTTAX,
                                      SETTLEDATE)                                        
                              VALUES (1,
                                      v_bsb,
                                      v_account_no,
                                      13,
                                      p_debit,
                                      v_title, 
                                      'N',
                                      v_lodgementnum, 
                                      '032-797 001006',
                                      'SMARTCARD TRANS', 
                                      '00000000', 
                                      sysdate);
  END;
  
  FUNCTION get_email_recipients(mail_conn IN OUT UTL_SMTP.connection) RETURN NUMBER IS
  --Purpose: Function collects a list of recipents to email the deskbank 
  --         summary to, and returns the number of recipients
  --Author:  Kazuki Yamada
  --Date:    03/JUN/2016
  
  CURSOR c_recipients IS
    SELECT VALUE FROM PARAMETER 
     WHERE KIND='EMAIL_ADDRESS'
       AND CODE='ASSGN_RECIPIENT'
       AND ACTIVE='Y';
  v_counter NUMBER := 0;         
  v_module_name VARCHAR2(30) := 'get_email_recipients';
  
  BEGIN
    announceme(v_module_name||'. Finding email(s) to send deskbank summary report to');
  
    FOR r_recipients IN c_recipients LOOP
      v_counter := v_counter + 1;
      UTL_SMTP.rcpt(mail_conn, r_recipients.VALUE);
      IF v_counter > 100 THEN EXIT;
      END IF;
    END LOOP;
    
    RETURN v_counter;
  END;

  FUNCTION get_lodgement_num RETURN VARCHAR2 IS
  --Purpose: Function returns unique lodgement number
  --Author:  Kazuki Yamada
  --Date:    18/MAY/2016  
  v_module_name  VARCHAR2(35) := 'get_lodgement_num';  
  BEGIN        
    Announceme(v_module_name||'. Generating new lodgement number for settlement');  
    RETURN TO_CHAR(TO_CHAR(sysdate, 'YYYYMMDD') || 
            LPAD(FSS_SEQ_LODGEMENTNR.NEXTVAL, 7, '0'));
  END; 
    
  FUNCTION format_currency(p_currency IN NUMBER) RETURN VARCHAR2 IS
  --Purpose: Function formats currency in cents to dollar format
  --Author:  Kazuki Yamada
  --Date:    21/MAY/2016
  BEGIN
    RETURN TO_CHAR(SUBSTR(p_currency,0,length(p_currency)-2)
                   ||'.'||
                   SUBSTR(p_currency, -2, 2));
  END; 
  
  /* PUBLIC PROCEDURES*/
  PROCEDURE DailySettlement IS
    --Purpose: Generate daily settlement to pay merchants
    --Author:  Kazuki Yamada
    --Date:    11/MAY/2016
    v_module_name VARCHAR2(35) := 'Daily_Setlement';
    v_run_msg     VARCHAR2(255);
    v_runLogID    NUMBER;
    
    v_run_record FSS_RUN_TABLE%ROWTYPE;  
  BEGIN
    Announceme(v_module_name||'. Settling transactions for: '||to_char(sysdate, 'DD-MM-YYYY'));
    v_run_ok := TRUE;
    
    check_run_table(v_runLogID, v_run_record);
    
    IF v_runLogID = -1 THEN
      v_run_msg := 'Program was ran at '||to_char(v_run_record.runstart);
      v_run_msg := v_run_msg||' and ended at '||to_char(v_run_record.runend);
      announceme(v_run_msg);
      
    ELSE     
      -- Collect new transactions if any
      upload_new_Transactions;
          
      settle_transactions; 
    END IF;
    
    -- The module has completed so update the FSS_RUN_TABLE table
    IF v_run_ok THEN
      UPDATE FSS_RUN_TABLE
        SET RUNEND      = sysdate,
            RUNOUTCOME  = 'SUCCESS',
            REMARKS     = 'Run completed successfully'
      WHERE RUNID = v_runLogID;
      COMMIT;
    END IF;
  END;

  PROCEDURE DailyBankingSummary(p_report_date IN DATE DEFAULT SYSDATE) IS
    --Purpose: This procedure generates daily banking summmary for the p_report_date
    --Author:  Kazuki Yamada
    --Date:    11/MAY/2016
    v_module_name       VARCHAR2(19) := 'DailyBankingSummary';
    v_filename          VARCHAR2(38) := 'DailyBankingSummary'||to_char(sysdate,'DD-Mon-YYYY')||'_KY.txt';
    v_deskbank_filename VARCHAR2(30) := 'DS_'||to_char(p_report_date,'DDMMYYYY')||'_KY.dat';
    v_fileline          VARCHAR2(120);  
    v_currency          VARCHAR2(12);
    v_total_credit      NUMBER :=0;
    v_total_debit       NUMBER :=0;
    v_counter           NUMBER :=0;
    v_file utl_file.file_type;
      
    CURSOR c_settlement IS
      SELECT MERCHANTID, MERCHANTTITLE, ACCOUNTNR, 
             SETTLEVALUE, TRAN_CODE
        FROM FSS_DAILY_SETTLEMENT
       WHERE TRUNC(SETTLEDATE) = TRUNC(P_REPORT_DATE)
         AND LODGEMENTNR IS NOT NULL;
        
    r_settle_record c_settlement%ROWTYPE;  
  BEGIN
    Announceme(v_module_name||'. Generating deskbank summary for: '||to_char(p_report_date, 'DD-Mon-YYYY'));
    
    v_file := utl_file.fopen ('KAYAMADA_DIR',v_filename,'W');
    
    -- Deskbank Summary titles
    v_fileline := LPAD(' ', 30, ' ') || 'SMARTCARD SETTLEMENT SYSTEM';
    utl_file.put_line(v_file, v_fileline);    
    v_fileline := LPAD(' ', 32, ' ') || 'DAILY DESKBANK SUMMARY';
    utl_file.put_line(v_file, v_fileline);
    
    -- Date and page number
    v_fileline := TO_CHAR('Date '||
                          TO_CHAR(sysdate,'DD-MM-YYYY')) ||
                          LPAD('Page 1', 76, ' ');    
    utl_file.put_line(v_file, v_fileline);
    
    -- Header row for summary
    utl_file.put_line(v_file, '');    
    v_fileline := RPAD('Merchant ID', 12, ' ')    ||
                  RPAD('Merchant Name', 33, ' ')  ||
                  RPAD('Account Number', 15, ' ') ||
                  LPAD('Debit', 15, ' ')          ||
                  LPAD('Credit', 16, ' ');
    utl_file.put_line(v_file, v_fileline);   
    
    v_fileline := RPAD('-', 11, '-')||' '||
                  RPAD('-', 32, '-')||' '||
                  RPAD('-', 14, '-')||' '||
                  RPAD('-', 15, '-')||' '||
                  RPAD('-', 15, '-');    
    utl_file.put_line(v_file, v_fileline);
    
    -- Print records    
    FOR r_settle_record IN c_settlement LOOP
      v_fileline := RPAD(NVL(to_char(r_settle_record.merchantid),' '), 12, ' ')||
                    RPAD(r_settle_record.MERCHANTTITLE, 33, ' ') ||
                    RPAD(r_settle_record.ACCOUNTNR, 15, ' ');
      v_currency := format_currency(r_settle_record.SETTLEVALUE);

      -- Check if debit or credit 
      IF r_settle_record.TRAN_CODE = 13 THEN
        v_total_debit := v_total_debit + r_settle_record.SETTLEVALUE;
        v_fileline := v_fileline || LPAD(v_currency, 15, ' ');
      ELSE
        v_total_credit := v_total_credit + r_settle_record.SETTLEVALUE;
        v_fileline := v_fileline ||
                      LPAD(' ', 15, ' ') ||
                      LPAD(v_currency, 16, ' ');
      END IF;
      utl_file.put_line(v_file, v_fileline);
      
      v_counter := v_counter + 1;
      EXIT WHEN v_counter > 100;
    END LOOP;
    
    v_fileline := RPAD(' ', 60, ' ')||
                  RPAD('-', 15, '-')||' '||
                  RPAD('-', 15, '-'); 
    utl_file.put_line(v_file, v_fileline);  

    -- Print totals
    v_currency := format_currency(v_total_debit);
    v_fileline := RPAD('BALANCE TOTAL', 60, ' ')||
                  LPAD(v_currency, 15, ' ')||' ';   

    v_currency := format_currency(v_total_credit);
    v_fileline := v_fileline||LPAD(v_currency, 15, ' ');
    utl_file.put_line(v_file, v_fileline);
    
    v_fileline := 'Deskbank file Name : '|| v_deskbank_filename;
    utl_file.put_line(v_file, v_fileline);
    
    v_fileline := 'Dispatch Date'||LPAD(': ',8,' ')|| to_char(p_report_date, 'DD Mon YYYY');
    utl_file.put_line(v_file, v_fileline);
   
    -- Print end of file
    utl_file.put_line(v_file, '');
    v_fileline := LPAD(' ', 30, ' ') ||'****** End of Report ******';
    utl_file.put_line(v_file, v_fileline);
    
    utl_file.fclose(v_file);
    
    email_summary(p_report_date);   
    
    
  END;
  
  PROCEDURE FraudReport IS
    --Purpose: This procedure will look for any discrepancies in transactions
    --Author:  Kazuki Yamada
    --Date:    20/May/2016
    v_module_name VARCHAR2(35) := 'FraudReport';
    v_comments    VARCHAR2(255);
    v_counter     NUMBER :=0;
    v_fraud_found BOOLEAN := FALSE;
    
    CURSOR c_cards IS
          SELECT LAG(trn.TRANSACTIONNR, 1) OVER (ORDER BY trn.CARDID, trn.TRANSACTIONDATE) prev_trn,
                  trn.TRANSACTIONNR,       
                  LAG(trn.CARDID, 1) OVER (ORDER BY trn.CARDID, trn.TRANSACTIONDATE) prev_card, 
                  trn.CARDID,
                  LAG(trn.CARDOLDVALUE, 1) OVER (ORDER BY trn.CARDID, trn.TRANSACTIONDATE) prev_val,
                  trn.CARDOLDVALUE,      
                  LAG(trn.TRANSACTIONDATE, 1) OVER (ORDER BY trn.CARDID, trn.TRANSACTIONDATE) prev_date,
                  trn.TRANSACTIONDATE,
                  trn.TRANSACTIONAMOUNT,
                  trn.CARDNEWVALUE
             FROM FSS_TRANSACTIONS trn
        FULL JOIN FSS_FRAUD fr ON trn.TRANSACTIONNR = fr.TRANSACTIONNR
            WHERE trn.TRANSACTIONNR <> DECODE(fr.TRANSACTIONNR, NULL,0) -- Omit previous fraudulent transactions where
               OR trn.TRANSACTIONNR <> DECODE(fr.PREVTRNNR, NULL,0);
    r_cards c_cards%ROWTYPE;
  BEGIN
    Announceme(v_module_name||'. Looking for frauduelnt transactions');
    
    FOR r_cards IN c_cards LOOP
      -- Checks if card value goes up
      IF r_cards.CARDID=r_cards.prev_card AND r_cards.CARDOLDVALUE>r_cards.prev_val THEN
        v_counter := v_counter + 1;
        v_comments := 'Card value is higher after transaction BEFORE: '||
                      r_cards.prev_val||
                      ' ('||
                      TO_CHAR(r_cards.prev_date,'DD-MON-YYYY HH24:MI')||
                      '), AFTER: '||
                      r_cards.CARDOLDVALUE||
                      ' ('||
                      TO_CHAR(r_cards.TRANSACTIONDATE,'DD-MON-YYYY HH24:MI')||
                      ')';
        v_fraud_found := TRUE;
      ELSIF r_cards.CARDNEWVALUE != (r_cards.CARDOLDVALUE-r_cards.TRANSACTIONAMOUNT) THEN
        v_comments := 'Transaction mismatch in: "'||
                      r_cards.CARDNEWVALUE||
                      ' =/= '||
                      r_cards.CARDOLDVALUE||
                      ' - '||
                      r_cards.TRANSACTIONAMOUNT||
                      ' ('||
                      TO_CHAR(r_cards.TRANSACTIONDATE,'DD-MON-YYYY HH24:MI')||
                      ')';
        v_fraud_found := TRUE;     
      END IF;
      IF v_fraud_found THEN
        v_counter := v_counter + 1;
        INSERT INTO FSS_FRAUD(TRANSACTIONNR,
                              PREVTRNNR,
                              CARDID,
                              CARDOLDVALUE,
                              PREVVALUE,
                              TRANSACTIONDATE,
                              PREVDATE,
                              TRANSACTIONAMOUNT,
                              CARDNEWVALUE,
                              COMMENTS)
                      VALUES (r_cards.TRANSACTIONNR,
                              r_cards.prev_trn,
                              r_cards.CARDID,
                              r_cards.CARDOLDVALUE,
                              r_cards.prev_val,
                              r_cards.TRANSACTIONDATE,
                              r_cards.prev_date,                              
                              r_cards.TRANSACTIONAMOUNT,
                              r_cards.CARDNEWVALUE,
                              v_comments);        
      END IF;
      v_fraud_found := FALSE;
    END LOOP;
    
    IF v_counter > 0 THEN
      create_fraud_report;
    END IF;    
  END;  
  
END PKG_FSS_SETTLEMENT;