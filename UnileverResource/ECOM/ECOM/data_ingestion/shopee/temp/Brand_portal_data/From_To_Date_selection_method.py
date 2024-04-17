# Databricks notebook source
def get_date_format(input_str_date):
    format = '%Y/%m/%d'
    date = datetime.datetime.strptime(input_str_date, format)
    return date.date()

# COMMAND ----------

def get_YearMonth(date):
    month = date.month
    if month <= 9:
        year_month = (str(date.year)+'-0'+str(date.month))
    else:
        year_month = (str(date.year)+'-'+str(date.month))
        
    return year_month

# COMMAND ----------

def month_check(date_form):
  if date_form.month < 10:
    return '0' + str(date_form.month)
  else:
    return str(date_form.month)
  
def day_check(date_form):
  if date_form.day < 10:
    return '0' + str(date_form.day)
  else:
    return str(date_form.day)

# COMMAND ----------

def select_FromTo_date(browser, from_date, to_date):
    
    now = datetime.datetime.now()
    year_month = get_YearMonth(from_date)
        
    #select from_date
    if now.year == from_date.year:
        if now.month == from_date.month:
            time.sleep(2)
            
            try:
                #select from_date
                from_date_str = str(from_date)
                print(from_date_str)
                q = browser.find_element_by_css_selector("[title^='" + from_date_str + "']")
                q.click()
                time.sleep(2)
                print('selected from_date')
            except Exception as error:
                print('Month or Day is not available for the from_date')
        else:
            try:
                #click on month tab
                browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[2]/div/div/div[1]/div/button[1]').click()
                time.sleep(2)
            except Exception as error:
                print('Month Tab is not available for the from_date')
            
            try:
                # click on respective month
                q = browser.find_element_by_css_selector("[title^='" + year_month + "']")
                q.click()
                time.sleep(2)
            except Exception as error:
                print('Month is not available for the from_date')
            
            try:
                #click on date
                from_date_str = str(from_date)
                q = browser.find_element_by_css_selector("[title^='" + from_date_str + "']")
                q.click()
                time.sleep(2)
                print('selected from_date')
            except Exception as error:
                print('DD is not available for the from_date')
            
    else:
        try:
            #click on year tab
            browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[2]/div/div/div[1]/div/button[2]').click()
            time.sleep(2)
        except Exception as error:
                print('Year Tab is not available for the from_date')
        
        try:
            #select year
            q = browser.find_element_by_css_selector("[title^='" + str(from_date.year) + "']")
            q.click()
        except Exception as error:
                print('Year is not available for the from_date')
        
        try:
            #click on month tab
            browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[2]/div/div/div[1]/div/button[1]').click()
            time.sleep(2)
        except Exception as error:
                print('Month Tab is not available for the from_date')
        
        try:
            # click on respective month
            q = browser.find_element_by_css_selector("[title^='" + year_month + "']")
            q.click()
            time.sleep(2)
        except Exception as error:
                print('Month is not available for the from_date')
        
        try:
            #click on date
            from_date_str = str(from_date)
            q = browser.find_element_by_css_selector("[title^='" + from_date_str + "']")
            q.click()
            time.sleep(2)
            print('selected from_date')
        except Exception as error:
                print('DD is not available for the from_date')
        
    #------------------------------------------------------------      
    now = datetime.datetime.now()    
    year_month = get_YearMonth(to_date)
    
    if from_date.year == to_date.year:
        try:
            #click on month tab for to_date
            browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[2]/div/div/div[1]/div/button[1]').click()
            time.sleep(2)
        except Exception as error:
            print('Month Tab is not available for to_date')
            
        try:
            # click on respective month for to_date
            q = browser.find_element_by_css_selector("[title^='" + year_month + "']")
            q.click()
            time.sleep(2)
        except Exception as error:
            print('Month is not available for to_date')
            
        try:
            #click on date
            to_date_str = str(to_date)
            q = browser.find_element_by_css_selector("[title^='" + to_date_str + "']")
            q.click()
            time.sleep(2)
            print('selected to_date')
        except Exception as error:
            print('DD is not available for to_date')            
        
    else:
        try:
            #print('click on next year Arrow')
            browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[2]/div/div/div[1]/button[4]').click()
            time.sleep(2)
        except Exception as error:
            print('next arrow button is not available for to_date')
           
        if from_date.month == to_date.month:
            try:
                to_date_str = str(to_date)
                q = browser.find_element_by_css_selector("[title^='" + to_date_str + "']")
                q.click()
                time.sleep(2)
                print('selected to_date')
            except Exception as error:
                print('DD is not available for to_date')
            
        else:
            try:
                #print('click on month list')
                browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[2]/div/div/div[1]/div/button[1]').click()
                time.sleep(2)
            except Exception as error:
                print('Month Tab is not working for to_date')
                
            try:
                # click on respective month
                q = browser.find_element_by_css_selector("[title^='" + year_month + "']")
                q.click()
                time.sleep(2)
            except Exception as error:
                print('month is not available for to_date')
            
            try:
                #click on date
                to_date_str = str(to_date)
                q = browser.find_element_by_css_selector("[title^='" + to_date_str + "']")
                q.click()
                time.sleep(2)
                print('selected to_date')
            except Exception as error:
                print('DD is not available for to_date')