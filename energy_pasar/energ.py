sdf['current_assets'] = sdf['total_assets'] - sdf['noncurrent_assets']
sdf['avg_assets'] = sdf[['init_assets' , 'total_assets']].mean(axis=1)
sdf['total_loans'] = sdf['long_term_loans'] + sdf['current_loans']

col = 'total_liabilities'
sdf[col] = sdf['current_liabilities'] + sdf['noncurrent_liabilities']
##


##
sdf = add_ratios(sdf , ratios)
##
fn = 'firms-names.xlsx'
fp = base_dir / 'data' / fn
nms = pd.read_excel(fp , engine='openpyxl')
##
msk = sdf['firm'].isin(nms['short_name'])
assert msk.all()
##
fp2 = base_dir / 'data' / 'tse-bs.xlsx'
bs = pd.read_excel(fp2 , engine='openpyxl')
##
bs['fiscal_year'] = bs['fiscal_year'].astype(int).astype(str)
bs = bs.sort_values(by='fiscal_year')
bs['jm'] = bs.fiscal_year.astype(int) // 100
bs['1_yr'] = bs['jm'].apply(lambda
                              x : find_n_jmonth_ahead(x , 12))

bs1 = bs[['symbol' , '1_yr' , 'total_assets']]

bs1 = bs1.rename(columns={
    'total_assets' : 'init_assets'
    })

bs1['1_yr'] = bs1['1_yr'].astype(int).astype(str)
bs['jm'] = bs['jm'].astype(int).astype(str)
bs = bs.drop(columns=['1_yr'])
##
bs2 = bs.merge(bs1 , left_on=['symbol' , 'jm'] , right_on=['symbol' , '1_yr'])
##
cols2keep = {
    'symbol'                 : None ,
    'fiscal_year'            : None ,
    'noncurrent_assets'      : None ,
    'init_assets'            : None ,
    'total_assets'           : None ,
    'shareholders_equity'    : None ,
    'noncurrent_liabilities' : None ,
    'current_liabilities'    : None ,
    }
bs2 = bs2[cols2keep.keys()]
##
fp = '/Users/mahdimir/Documents/portfos_data/m_helper_data/Cleaned_Stock_Prices_1400_06_16.parquet'
df1 = pd.read_parquet(fp)
##
df2 = df1[df1['jalaliDate'].eq(13971228)]
##
df2 = df2.sort_values(by='MarketCap' , ascending=False)
df2 = df2.iloc[:300]
##
bs2 = bs2[bs2['fiscal_year'].astype(int).ge(13970620)]
##
bs2 = bs2[bs2['symbol'].isin(df2['name'])]
##
bs2 = bs2[bs2['shareholders_equity'].ge(0)]
##


fp = base_dir / 'data' / 'tse-profits-97.xlsx'
pr = pd.read_excel(fp , engine='openpyxl')
##
pr['fiscal_year'] = pr['سال مالی'].apply(find_jdate_as_int_using_re)
##
colspr = {
    'symbol'        : 'symbol' ,
    'fiscal_year'   : 'fiscal_year' ,
    'op_rev'        : 'جمع درآمدها' ,
    'gross_profits' : 'سود ناویژه' ,
    'op_profits'    : 'سود (زیان) عملیاتی' ,
    'fiscal_costs'  : 'هزینه‌های مالی' ,
    'net_profits'   : 'سود (زیان) پس از کسر مالیات' ,
    }
cols2 = colspr.values()
pr = pr[cols2]
pr = pr.rename(columns={y : x for x , y in colspr.items()})
##
bs2 = bs2.merge(pr , on=['symbol' , 'fiscal_year'] , how='left')
##
bs2['current_assets'] = bs2['total_assets'] - bs2['noncurrent_assets']
bs2['total_liabilities'] = bs2['noncurrent_liabilities'] + bs2[
  'current_liabilities']
bs2['avg_assets'] = bs2[['init_assets' , 'total_assets']].mean(axis=1)
##
ratios = {
    'current_ratio' : ('current_assets' , 'current_liabilities') ,
    'debt_ratio'    : ('total_liabilities' , 'total_assets') ,
    'avg_ROA'       : ('net_profits' , 'avg_assets') ,
    'ROE'           : ('net_profits' , 'shareholders_equity') ,
    }
##
bs2 = add_ratios(bs2 , ratios)

##
def return_tse_avg_roa(
    df ,
    coln='avg_ROA'
    ) :
  x = filter_nan_and_outliers(df[coln])
  x = x * 100

  sns.set_style('white')
  ax = sns.kdeplot(x , label='TSE' , fill=True)
  ax.set_xlabel('ROA')
  plt.axvline(x.mean() , linestyle='--' , label='mean' , color='r')
  plt.axvline(x.median() , linestyle='--' , label='median' , color='b')
  plt.axvline(x.quantile(.1) ,
              linestyle='--' ,
              label='10% Percentile' ,
              color='g')
  plt.axvline(x.quantile(.9) ,
              linestyle='--' ,
              label='90% Percentile' ,
              color='m')
  ax.xaxis.set_major_formatter(FuncFormatter(lambda
                                               y ,
                                               _ : "{:g}%".format(y)))
  ax.legend()
  return ax

##
plt.close()
fig_dir = base_dir / 'figs'
fp = fig_dir / 'tse-avg-ROA.png'
ax1 = return_tse_avg_roa(bs2)
plt.savefig(fp)
plt.close()
##
sdf['eng_name'] = sdf['firm'].map(nms.set_index('short_name')['eng_name'])
##

##
gost = 'گسترش انرژی پاسارگاد'
msk = group_data['firm'].eq(gost)
ind1 = msk[msk].index[0]
glbl = get_display(reshape(gost))
##
for ind , ser in group_data.iterrows() :
  ax2 = return_tse_avg_roa(bs2)
  lbl = get_display(reshape(ser['firm']))
  plt.axvline(ser['ROA'] * 100 , label=lbl , color='k')
  if lbl != glbl :
    gost_val = group_data.at[ind1 , 'ROA']
    plt.axvline(gost_val , label=glbl , color='g')
  plt.legend()
  fp = fig_dir / ('ROA-' + str(ser['eng_name']) + '.png')
  plt.savefig(fp)
  plt.close()

##
def return_tse_debt_ratio(
    df ,
    coln='debt_ratio'
    ) :
  x = filter_nan_and_outliers(df[coln])
  sns.set_style('white')
  ax = sns.kdeplot(x , label='TSE' , fill=True , cut=0)
  ax.set_xlabel('Debt Ratio')
  plt.axvline(x.mean() , linestyle='--' , label='mean' , color='r')
  plt.axvline(x.median() , linestyle='--' , label='median' , color='b')
  plt.axvline(x.quantile(.1) ,
              linestyle='--' ,
              label='10% Percentile' ,
              color='g')
  plt.axvline(x.quantile(.9) ,
              linestyle='--' ,
              label='90% Percentile' ,
              color='m')
  ax.legend()
  return ax

##
plt.close()
fp = fig_dir / 'tse-debt_ratio.png'
ax1 = return_tse_debt_ratio(bs2)
plt.savefig(fp)
plt.close()
##
for ind , ser in group_data.iterrows() :
  ax2 = return_tse_debt_ratio(bs2)
  lbl = get_display(reshape(ser['firm']))
  plt.axvline(ser['debt_ratio'] , label=lbl , color='k')
  if lbl != glbl :
    gost_val = group_data.at[ind1 , 'debt_ratio']
    plt.axvline(gost_val , label=glbl , color='g')
  plt.legend()
  fp = fig_dir / ('debt-ratio-' + str(ser['eng_name']) + '.png')
  plt.savefig(fp)
  plt.close()

##

##
def return_tse_roe(
    df
    ) :
  x = filter_nan_and_outliers(df['ROE'])
  x = x * 100

  sns.set_style('white')
  ax = sns.kdeplot(x , label='TSE' , fill=True)
  ax.set_xlabel('ROE')
  plt.axvline(x.mean() , linestyle='--' , label='mean' , color='r')
  plt.axvline(x.median() , linestyle='--' , label='median' , color='b')
  plt.axvline(x.quantile(.1) ,
              linestyle='--' ,
              label='10% Percentile' ,
              color='g')
  plt.axvline(x.quantile(.9) ,
              linestyle='--' ,
              label='90% Percentile' ,
              color='m')
  ax.xaxis.set_major_formatter(FuncFormatter(lambda
                                               y ,
                                               _ : "{:g}%".format(y)))
  ax.legend()
  return ax

##
plt.close()
fp = fig_dir / 'tse-ROE.png'
ax1 = return_tse_roe(bs2)
plt.savefig(fp)
plt.close()
##
for ind , ser in group_data.iterrows() :
  ax2 = return_tse_roe(bs2)
  lbl = get_display(reshape(ser['firm']))
  plt.axvline(ser['ROE'] * 100 , label=lbl , color='k')
  if lbl != glbl :
    gost_val = group_data.at[ind1 , 'ROE']
    plt.axvline(gost_val * 100 , label=glbl , color='g')
  plt.legend()
  fp = fig_dir / ('ROE-' + str(ser['eng_name']) + '.png')
  plt.savefig(fp)
  plt.close()

##

##
def return_tse_current_ratio(
    df
    ) :
  x = filter_nan_and_outliers(df['current_ratio'])

  sns.set_style('white')
  ax = sns.kdeplot(x , label='TSE' , fill=True , cut=True)
  ax.set_xlabel('Current Ratio')
  plt.axvline(x.mean() , linestyle='--' , label='mean' , color='r')
  plt.axvline(x.median() , linestyle='--' , label='median' , color='b')
  plt.axvline(x.quantile(.1) ,
              linestyle='--' ,
              label='10% Percentile' ,
              color='g')
  plt.axvline(x.quantile(.9) ,
              linestyle='--' ,
              label='90% Percentile' ,
              color='m')
  ax.legend()
  return ax

##
plt.close()
fp = fig_dir / 'tse-current-ratio.png'
ax1 = return_tse_current_ratio(bs2)
plt.savefig(fp)
plt.close()
##
for ind , ser in group_data.iterrows() :
  ax2 = return_tse_current_ratio(bs2)
  lbl = get_display(reshape(ser['firm']))
  plt.axvline(ser['current_ratio'] , label=lbl , color='k')
  if lbl != glbl :
    gost_val = group_data.at[ind1 , 'current_ratio']
    plt.axvline(gost_val , label=glbl , color='g')
  plt.legend()
  fp = fig_dir / ('current-ratio-' + str(ser['eng_name']) + '.png')
  plt.savefig(fp)
  plt.close()

##
dt = pr.merge(bs2 , on=['symbol' , 'fiscal_year'])

##

##
msk = group_data['op_profits_margin'].notna()
x_pos = list(range(len(msk[msk])))
y = group_data.loc[msk , 'op_profits_margin'] * 100
barlist = plt.bar(x_pos , y , color='orange')

firms = list(group_data.loc[msk , 'firm'])
ind1 = firms.index(gost)
barlist[ind1].set_color('b')

xticks = [get_display(reshape(x)) for x in group_data.loc[msk , 'firm']]

plt.xticks(x_pos , xticks , rotation=90 , fontsize=8)
ax = plt.gca()
ax.yaxis.set_major_formatter(FuncFormatter(lambda
                                             y ,
                                             _ : "{:g}%".format(y)))

for bar in barlist :
  yval = bar.get_height()
  plt.text(bar.get_x() , yval + 3 , str(round(yval)))

ax.set_ylabel('Operating Profits Margin (%)')

plt.savefig(fig_dir / 'op_margin.png' , bbox_inches='tight' , dpi=1000)
plt.close()

##
plt.close()
msk = group_data['gross_profits_margin'].notna()
x_pos = list(range(len(msk[msk])))
y = group_data.loc[msk , 'gross_profits_margin'] * 100
barlist = plt.bar(x_pos , y , color='orange')

firms = list(group_data.loc[msk , 'firm'])
ind1 = firms.index(gost)
barlist[ind1].set_color('b')

xticks = [get_display(reshape(x)) for x in group_data.loc[msk , 'firm']]

plt.xticks(x_pos , xticks , rotation=90 , fontsize=8)
ax = plt.gca()
ax.yaxis.set_major_formatter(FuncFormatter(lambda
                                             y ,
                                             _ : "{:g}%".format(y)))

for bar in barlist :
  yval = bar.get_height()
  if yval >= 0 :
    plt.text(bar.get_x() , yval + 3 , str(round(yval)))
  elif yval < 0 :
    plt.text(bar.get_x() , yval - 10 , str(round(yval)))

ax.set_ylabel('Gross Profits Margin (%)')

plt.savefig(fig_dir / 'gross_p_margin.png' , bbox_inches='tight' , dpi=1000)
plt.close()

##
plt.close()
msk = group_data['net_profits_margin'].notna()
x_pos = list(range(len(msk[msk])))
y = group_data.loc[msk , 'net_profits_margin'] * 100
barlist = plt.bar(x_pos , y , color='orange')

firms = list(group_data.loc[msk , 'firm'])
ind1 = firms.index(gost)
barlist[ind1].set_color('b')

xticks = [get_display(reshape(x)) for x in group_data.loc[msk , 'firm']]

plt.xticks(x_pos , xticks , rotation=90 , fontsize=8)
ax = plt.gca()
ax.yaxis.set_major_formatter(FuncFormatter(lambda
                                             y ,
                                             _ : "{:g}%".format(y)))

for bar in barlist :
  yval = bar.get_height()
  if yval >= 0 :
    plt.text(bar.get_x() , yval + 3 , str(round(yval)))
  elif yval < 0 :
    plt.text(bar.get_x() , yval - 10 , str(round(yval)))

ax.set_ylabel('Net Profits Margin (%)')

plt.savefig(fig_dir / 'net_p_margin.png' , bbox_inches='tight' , dpi=1000)
plt.close()

##
plt.close()
msk = group_data['current_ratio'].notna()
x_pos = list(range(len(msk[msk])))
y = group_data.loc[msk , 'current_ratio']
barlist = plt.bar(x_pos , y , color='orange')

firms = list(group_data.loc[msk , 'firm'])
ind1 = firms.index(gost)
barlist[ind1].set_color('b')

xticks = [get_display(reshape(x)) for x in group_data.loc[msk , 'firm']]

plt.xticks(x_pos , xticks , rotation=90 , fontsize=8)
ax = plt.gca()
ax.yaxis.set_major_formatter(FuncFormatter(lambda
                                             y ,
                                             _ : "{:g}  ".format(y)))

for bar in barlist :
  yval = bar.get_height()
  if yval >= 0 :
    plt.text(bar.get_x() , yval + 0.1 , str(round(yval)))
  elif yval < 0 :
    plt.text(bar.get_x() , yval - 10 , str(round(yval)))

ax.set_ylabel('Current Ratio')

plt.savefig(fig_dir / 'current_ratio.png' , bbox_inches='tight' , dpi=1000)
plt.close()

##
plt.close()
msk = group_data['debt_ratio'].notna()
x_pos = list(range(len(msk[msk])))
y = group_data.loc[msk , 'debt_ratio']
barlist = plt.bar(x_pos , y , color='orange')

firms = list(group_data.loc[msk , 'firm'])
ind1 = firms.index(gost)
barlist[ind1].set_color('b')

xticks = [get_display(reshape(x)) for x in group_data.loc[msk , 'firm']]

plt.xticks(x_pos , xticks , rotation=90 , fontsize=8)
ax = plt.gca()
ax.yaxis.set_major_formatter(FuncFormatter(lambda
                                             y ,
                                             _ : "{:g}  ".format(y)))

for bar in barlist :
  yval = bar.get_height()
  if yval >= 0 :
    plt.text(bar.get_x() , yval + 0.05 , str(round(yval , 1)) , fontsize=6)
  elif yval < 0 :
    plt.text(bar.get_x() , yval - 10 , str(round(yval)))

ax.set_ylabel('Debt Ratio')

plt.savefig(fig_dir / 'debt_ratio.png' , bbox_inches='tight' , dpi=1000)
plt.close()

##
plt.close()
msk = group_data['ROA'].notna()
x_pos = list(range(len(msk[msk])))
y = group_data.loc[msk , 'ROA'] * 100
barlist = plt.bar(x_pos , y , color='orange')

firms = list(group_data.loc[msk , 'firm'])
ind1 = firms.index(gost)
barlist[ind1].set_color('b')

xticks = [get_display(reshape(x)) for x in group_data.loc[msk , 'firm']]

plt.xticks(x_pos , xticks , rotation=90 , fontsize=8)
ax = plt.gca()
ax.yaxis.set_major_formatter(FuncFormatter(lambda
                                             y ,
                                             _ : "{:g}%".format(y)))

for bar in barlist :
  yval = bar.get_height()
  if yval >= 0 :
    plt.text(bar.get_x() , yval + 1 , str(round(yval , 1)) , fontsize=6)
  elif yval < 0 :
    plt.text(bar.get_x() , yval - 3 , str(round(yval)))

ax.set_ylabel('ROA (%)')

plt.savefig(fig_dir / 'roa.png' , bbox_inches='tight' , dpi=1000)
plt.close()

##
plt.close()
msk = group_data['ROE'].notna()
x_pos = list(range(len(msk[msk])))
y = group_data.loc[msk , 'ROE'] * 100
barlist = plt.bar(x_pos , y , color='orange')

firms = list(group_data.loc[msk , 'firm'])
ind1 = firms.index(gost)
barlist[ind1].set_color('b')

xticks = [get_display(reshape(x)) for x in group_data.loc[msk , 'firm']]

plt.xticks(x_pos , xticks , rotation=90 , fontsize=8)
ax = plt.gca()
ax.yaxis.set_major_formatter(FuncFormatter(lambda
                                             y ,
                                             _ : "{:g}%".format(y)))

for bar in barlist :
  yval = bar.get_height()
  if yval >= 0 :
    plt.text(bar.get_x() , yval + 1 , str(round(yval , 1)) , fontsize=6)
  elif yval < 0 :
    plt.text(bar.get_x() , yval - 6 , str(round(yval)))

ax.set_ylabel('ROE (%)')

plt.savefig(fig_dir / 'roe.png' , bbox_inches='tight' , dpi=1000)
plt.close()

##
fp = base_dir / 'سهامداران-۹۹.xlsx'
wb_dct = pd.read_excel(fp , engine='openpyxl' , sheet_name=None)

##
tdf = wb_dct[list(wb_dct.keys())[1]]
com = list(tdf.columns)[0]
gost = 'گسترش انرژی'
msk = tdf[com].eq(gost)
ind = msk[msk].index
shares = tdf.at[ind[0] , 'shares']
pct = shares / tdf['shares'].sum()

##
shares_dct = {}

for shdf in wb_dct.values() :
  com = list(shdf.columns)[0]
  msk = shdf[com].eq(gost)
  ind = msk[msk].index
  shares = shdf.at[ind[0] , 'shares']
  pct = shares / shdf['shares'].sum()
  shares_dct[com] = (shares , pct)

##
df = pd.DataFrame()

for ky , val in shares_dct.items() :
  df.at[ky , 'shares'] = val[0]
  df.at[ky , 'pct'] = val[1]
##
df.to_excel('pcts.xlsx')

##
fp = '/Users/mahdimir/Dropbox/R-Heidari-Mir/انرژی پاسارگاد/data/ownership.xlsx'
df = pd.read_excel(fp , engine='openpyxl')

##
unique_cols_0 = ['shareholder' , 'firm' , 'shares' , 'date']
df = df.drop_duplicates(subset=unique_cols_0)
##
unique_cols_0 = ['shareholder' , 'firm' , 'pct' , 'date']
df = df.drop_duplicates(subset=unique_cols_0)
##
msk = df['pct'].apply(type).isin([int , float])
##
msk1 = df.loc[msk , 'pct'].lt(1)
ind1 = msk1[msk1].index
##
df.loc[ind1 , 'pct'] = '<1'
##
df['pct'] = df['pct'].apply(lambda
                              x : '<1' if x == '<۱' else x)

##
df.to_excel(fp , index=False)

##
fp = '/Users/mahdimir/Dropbox/R-Heidari-Mir/انرژی پاسارگاد/data/tse-ratios.xlsx'
df = pd.read_excel(fp , engine='openpyxl')
##
df['pmarg'] = df['net_profits'] / df['op_rev']

##
x = filter_nan_and_outliers(df['pmarg'])
x = x * 100
##
plt.close()
sns.set_style('white')
ax = sns.kdeplot(x , label='TSE' , fill=True)
ax.set_xlabel('ROA')
plt.axvline(x.mean() , linestyle='--' , label='mean' , color='r')
plt.axvline(x.median() , linestyle='--' , label='median' , color='b')
plt.axvline(x.quantile(.1) ,
            linestyle='--' ,
            label='10% Percentile' ,
            color='g')
plt.axvline(x.quantile(.9) ,
            linestyle='--' ,
            label='90% Percentile' ,
            color='m')
# ax.xaxis.set_major_formatter(FuncFormatter(lambda y , _ : "{:g}%".format(y)))
ax.legend()
plt.show()
plt.savefig('pmarg.png')

##

cols_dct = {
    'firm'                   : 'شرکت' ,
    'fiscal_year'            : 'سال مالی منتهی به' ,
    'op_rev'                 : 'درآمد عملیاتی' ,
    'gross_profits'          : 'سود ناخالص' ,
    'op_profits'             : 'سود عملیاتی' ,
    'fiscal_costs'           : 'هزینه های مالی' ,
    'net_profits'            : 'سود خالص' ,
    'noncurrent_assets'      : 'دارایی های غیرجاری' ,
    'assets'                 : 'دارایی' ,
    'equity'                 : 'حقوق مالکانه' ,
    'long_term_loans'        : 'تسهیلات بلند مدت' ,
    'noncurrent_liabilities' : 'بدهی های غیر جاری' ,
    'current_loans'          : 'تسهیلات جاری' ,
    'current_liabilities'    : 'بدهی های جاری' ,
    'نظر حسابرس'             : 'نظر حسابرس' ,
    }

fa_cols = {
    'short_name'           : 'شرکت' ,
    'fiscal_year'          : 'سال مالی منتهی به' ,
    'op_rev'               : 'درآمد عملیاتی-میلیارد ریال' ,
    'gross_profits'        : 'سود ناخالص' ,
    'op_profits'           : 'سود عملیاتی' ,
    'net_profits'          : 'سود خالص' ,
    'current_assets'       : 'دارایی جاری' ,
    'assets'               : 'دارایی' ,
    'current_liabilities'  : 'بدهی های جاری' ,
    'total_liabilities'    : 'بدهی' ,
    'equity'               : 'حقوق مالکانه' ,
    'gross_profits_margin' : 'حاشیه سود ناخالص' ,
    'op_profits_margin'    : 'حاشیه سود عملیاتی' ,
    'net_profits_margin'   : 'حاشیه سود خالص' ,
    'ROA'                  : 'ROA' ,
    'ROE'                  : 'ROE' ,
    'current_ratio'        : 'نسبت جاری' ,
    'debt_ratio'           : 'نسبت بدهی' ,
    'نظر حسابرس'           : 'نظر حسابرس' ,
    }

def filter_nan_and_outliers(
    iser: pd.Series ,
    param=3
    ) :
  iser = iser[iser.notna()]
  z_scores = stats.zscore(iser)
  abs_z_scores = np.abs(z_scores)
  filter_outliers = abs_z_scores < param
  return iser[filter_outliers]

def find_jdate_as_int_using_re(
    ist
    ) :
  ist = digits.fa_to_en(str(ist))
  match_obj = re.search(r'1[3-4]\d{2}/?[0-1]\d/?[0-3]\d' , ist)
  if match_obj :
    match = match_obj.group()
    num = re.sub(r'\D' , '' , match)
    return str(int(num))

def find_n_jmonth_ahead(
    cur_month ,
    howmany=1
    ) :
  if howmany == 1 :
    if cur_month % 100 == 12 :
      next_month = (cur_month // 100 + 1) * 100 + 1
    else :
      next_month = cur_month + 1
    return next_month

  else :
    next_month = find_n_jmonth_ahead(cur_month)
    return find_n_jmonth_ahead(next_month , howmany - 1)
