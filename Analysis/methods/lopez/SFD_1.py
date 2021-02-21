#!/usr/bin/env python
# On 20131225 by lopezdeprado@lbl.gov
import networkx as nx,pandas as pd
#-----------------------------------------------------------------------
def SFD_AR(p):
    # Generate AR figures
    D=nx.DiGraph()
    for k in range(p):
        D.add_node('v'+str(k+1),shape='diamond')
        D.add_edge('v'+str(k),'v'+str(k+1),weight=1, \
            label='o'+str(k),style='dashed',arrowhead='onormal') # outbound arc
        D.add_edge('v'+str(k+1),'v0',weight=1, \
            label='a'+str(k+1)) # inbound arc
    return D
#-----------------------------------------------------------------------
def SFD_VAR(p,n):
    # Generate VAR figures
    D=nx.DiGraph()
    range1=range(1,n+1)
    for i in range1:
        for k in range(p):
            D.add_node('v('+str(i)+','+str(k+1)+')',shape='diamond')    
            D.add_edge('v('+str(i)+','+str(k)+')', \
                'v('+str(i)+','+str(k+1)+')',weight=1, \
                label='o('+str(i)+','+str(k)+')',style='dashed',arrowhead='onormal') # outbound
            for j in range1:
                D.add_edge('v('+str(i)+','+str(k+1)+')', \
                    'v('+str(j)+',0)',weight=1, \
                    label='a('+str(i)+','+str(k+1)+','+str(j)+')') # inbound arc
    return D
#-----------------------------------------------------------------------
def SFD_SVAR(p,n):
    # Generate SVAR figures
    D=nx.DiGraph()
    range1=range(1,n+1)
    for i in range1:
        range2=range1[:];range2.remove(i)
        for j in range2:
            D.add_edge('v('+str(i)+',0)','v('+str(j)+',0)',weight=1, \
                label='a('+str(i)+',0,'+str(j)+')',arrowhead='inv') # inbound sync arc
        for k in range(p): 
            D.add_node('v('+str(i)+','+str(k+1)+')',shape='diamond')
            D.add_edge('v('+str(i)+','+str(k)+')', \
                'v('+str(i)+','+str(k+1)+')',weight=1, \
                label='o('+str(i)+','+str(k)+')',style='dashed',arrowhead='onormal') # outbound
            for j in range1:
                D.add_edge('v('+str(i)+','+str(k+1)+')', \
                    'v('+str(j)+',0)',weight=1, \
                    label='a('+str(i)+','+str(k+1)+','+str(j)+')') # inbound arc
    return D
#-----------------------------------------------------------------------
def formGraphFromBetas(betas,aR2):
    # Create a graph using the coefficients matrix as an adjacency matrix
    D,lagNodes=nx.DiGraph(),[]
    # inbound arcs
    for i in betas.index.tolist():
        for j in betas.columns.tolist():
            if betas[j][i]!=0:
                if betas[j][i]<0:color='red'
                else:color='darkgreen'
                width=getWidth(aR2.ix[i][0])
                if '_' in j:
                    lagNodes.append((j[:j.find('_')],int(j[j.find('_')+1:])))
                    D.add_edge(j,i,weight=betas[j][i],arrowsize=1,color=color, \
                        penwidth=width) # inbound arc  
                else:
                    D.add_edge(j,i,weight=betas[j][i],arrowsize=1,color=color, \
                        penwidth=width,arrowhead='inv') # inbound sync arc
    # outbound arcs
    if len(lagNodes)>0:
        lagNodes=pd.DataFrame(lagNodes)
        groups=lagNodes.groupby([0]).groups
        for i in groups.keys():
            maxLag=max(lagNodes[1].ix[groups[i]])
            for k in range(0,maxLag):
                if k==0:j=i
                else:j=i+'_'+str(k)
                D.add_node(i+'_'+str(k+1),shape='diamond')
                D.add_edge(j,i+'_'+str(k+1),weight=1,style='dashed', \
                    arrowhead='onormal') # outbound arc
    return D
#-----------------------------------------------------------------------
def getWidth(value):
    value_=abs(value)/(1+value**2)**.5
    return value_*20+1
