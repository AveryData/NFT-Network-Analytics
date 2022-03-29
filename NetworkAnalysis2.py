# -*- coding: utf-8 -*-
"""
Created on Fri Mar 25 14:22:43 2022

@author: avery smith 
"""

# Import libraries
import sqlite3
import pandas as pd 
import networkx as nx
from pyvis.network import Network

# Connect to SQL 
con = sqlite3.connect('nfts.sqlite')
cur = con.cursor()


# Get a list of the 5 biggest projects
# get volume of transfers and sort
df1 = pd.read_sql_query("""
                        SELECT name, nft_address, SUM(transaction_value/1e18) AS volume
                        FROM transfers 
                        INNER JOIN nfts ON transfers.nft_address = nfts.address 
                        
                        
                        GROUP BY transfers.nft_address
                      
                        ORDER BY volume DESC
                       
                       
                        LIMIT 5
                       
                        """, con)


# nft contract adress dictionary 
contract_names_dict = dict(zip(df1.nft_address, df1.name))
# address tuple
contracts = tuple(contract_names_dict.keys())


# Get a list of all of the nft project names and addresses
all_project_names = pd.read_sql_query("""
                        SELECT * FROM nfts
                        
                        LIMIT 100000

                       
                        """, con)

# make a dictionary to map names to contractgs 
contract_names_dict_all = dict(zip(all_project_names['address'], all_project_names['name']))

# See what wallets own products in all proudcts
# Take the top n wallet owners
top_n_owners_list = pd.read_sql_query("""
                            SELECT COUNT(DISTINCT nft_address) AS num_projects , owner
                            FROM current_owners
                            WHERE nft_address IN  {}
                            
                            GROUP BY owner
                            ORDER BY num_projects DESC
                            
                            LIMIT 3
                             
                            """.format(contracts), con) 

# Create tuple for these "big owners"
owners_tuples = tuple( top_n_owners_list['owner'])



# Go through the big owners wallet and see everything they own
top_projects = pd.read_sql_query("""
                        SELECT nft_address, COUNT(owner) AS count FROM current_owners
                        WHERE owner IN {}
                        GROUP BY nft_address
                        ORDER BY count DESC
                        LIMIT 50000""".format(owners_tuples), con) 
                        
top_projects_tuple = tuple(top_projects['nft_address'])


# get all the NFTs for those top projects
all_nfts_in_top_projects = pd.read_sql_query("""
                        SELECT * FROM current_owners
                        WHERE nft_address IN {}

                        """.format(top_projects_tuple), con) 
                        
                        
                                         
                        
# Create the edge table
edge_table = pd.read_sql_query("""
                        SELECT t1.nft_address AS NFT1, t2.nft_address AS NFT2, COUNT(*) AS COUNT
                        
                        FROM current_owners AS t1
                        
                        INNER JOIN current_owners AS t2
                        ON t1.owner = t2.owner
                        

                        
                        WHERE t1.owner in {}
                        AND
                        NFT1 < NFT2
                        
                        GROUP BY NFT1, NFT2
                        HAVING COUNT(*) > 50
                        
    
    
    """.format(owners_tuples), con
    )



# Adding the names instead of the contract
edge_table['NFT1'] = edge_table['NFT1'].map(contract_names_dict_all)
edge_table['NFT2'] = edge_table['NFT2'].map(contract_names_dict_all)

# get rid of empty nodes
edge_table = edge_table.dropna()

# Rename columns
edge_table.columns = ['Source','Target','Weight']

sources = edge_table['Source']
targets = edge_table['Target']
weights = edge_table['Weight']
edge_data = zip(sources, targets, weights)



# Start Visualization
network_graph = Network(height='750px', width='100%', bgcolor='#222222', font_color='white')


# Adding all the partys 
for e in edge_data:
    src = e[0]
    dst = e[1]
    w = e[2]

    network_graph.add_node(src, src, title=src)
    network_graph.add_node(dst, dst, title=dst)
    network_graph.add_edge(src, dst, value=w)

# add neighbor data to node hover data
neighbor_map = network_graph.get_adj_list()
for node in network_graph.nodes:
    node['title'] += ' Neighbors:<br>' + '<br>'.join(neighbor_map[node['id']])
    node['value'] = len(neighbor_map[node['id']])

# display graph
network_graph.show('NFTMap.html')

