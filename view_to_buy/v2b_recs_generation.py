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

def V2BSeggregation (df, seggregatorCol, buyEvent, viewEvent):
    # Segregate Events array from the dataframe into 'views' and 'buys' (after removing suffix) for seggregator column
    import re
    df = df[[seggregatorCol]]
    df['viewItems'] = df[seggregatorCol].apply(lambda x: [re.sub(re.escape("_"+viewEvent), '', y) for y in x if y.endswith("_"+viewEvent)])
    df['buyItems'] = df[seggregatorCol].apply(lambda x: [re.sub(re.escape("_"+buyEvent), '', y) for y in x if y.endswith("_"+buyEvent)]) 
    df['viewCount'] = df[seggregatorCol].apply(lambda x: len([y for y in x if y.endswith('_'+viewEvent)]))
    df['buyCount'] = df[seggregatorCol].apply(lambda x: len([y for y in x if y.endswith('_'+buyEvent)]))
    df = df[(df['viewCount'] > 0) & (df['buyCount'] > 0)]
    df = df.drop(['viewCount', 'buyCount', seggregatorCol], axis=1)
    return(df)
