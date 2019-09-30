#################################################################################################
#                                                                                               #
# Copyright © 2019 Acoustic, L.P. All rights reserved.                                          #
#                                                                                               #
# NOTICE: This file contains material that is confidential and proprietary to                   #
# Acoustic, L.P. and/or other developers. No license is granted under any intellectual or       #
# industrial property rights of Acoustic, L.P. except as may be provided in an agreement with   #
# Acoustic, L.P. Any unauthorized copying or distribution of content from this file is          #
# prohibited.                                                                                   #
#                                                                                               #
#################################################################################################

def BasketPrep(df, useridCol, eventCol, viewEvent, buyEvent, itemidCol):
    # Event data stacking with Session & User specific
    df['item_event'] = df[itemidCol] + "_" + df[eventCol]
    dfPivot = df.groupby([useridCol, 'V2BSession'])
    baskets = dfPivot.apply(lambda x: x['item_event'].unique().tolist()).to_frame()
    baskets.reset_index(inplace=True)
    baskets.columns = [useridCol, 'V2BSession', 'products']
    baskets['viewCount'] = baskets['products'].apply(lambda x: len([y for y in x if y.endswith('_' + viewEvent)]))
    baskets['buyCount'] = baskets['products'].apply(lambda x: len([y for y in x if y.endswith('_' + buyEvent)]))
    baskets = baskets[(baskets['viewCount'] > 0) & (baskets['buyCount'] > 0)]
    return (baskets)
