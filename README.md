# DataFlowExamples


USECASE:
==========================================================================================================

 

Information:

 

Data:

 

+3 - win

+1 - draw

0 - loss

 

============================================================================================================

 

Team_id,Team_name,Group_id

1,Brazil,1

2,England,1

3,Uruguay,2

4,France,2

5,India,1

6,Australia,2

7,Italy,1

8,Nigeria,2

 

 

Stage_id,Stage_name

1,Group_stage

2,Semi_finals

3,3rd_place

4,Final

 

 

Group_id,Group Name

1,Team A

2,Team B

 

Match_id,Team1,Team2,Team1_goals,Team2_goals,Stage_id

1,1,2,3,2,1

2,1,5,0,0,1

3,1,7,4,3,1

4,2,5,2,0,1

5,2,7,2,2,1

6,5,7,3,3,1

7,3,4,2,1,1

8,3,6,3,1,1

9,3,8,3,2,1

10,4,6,5,1,1

11,4,8,2,1,1

12,6,8,2,2,1

13,1,4,3,2,2

14,3,2,2,3,2

15,4,1,1,2,3

16,2,1,1,2,4

 

 

===================================================================================================

 

Information:

 

 

(1,2,5,7) --> (1,2)

                     -->(2,1)

(3,4,6,8) --> (3,4)

 

 

 

1-3+1+3=7

2-0+3+1=4

5-1+0+1=2

7-0+1+1=2

 

3-3+3+3=9

4-0+3+3=6

6-0+0+1=1

8-0+0+1=1

 

====================================================================================================

 

 

Q. Construct points table from above information

 

Format:-

 

Team_name,Points

 

Q. Name highest goal scorer from each group in Group_Stage

 

Format:-

 

Group_id,Team_Name

 

Q. Name bottom goal scorer from each group in Group_Stage

 

Format:-

 

Group_id,Team_Name

 

Q. Name all semifinalists

 

Team_Name,points

 

Q. Who won the finals?

 

Team_Name,points

 

Q. Number of goals by each team scored in each stage

 

Team_Name,Group_id,No_of_goals

 

Add other relevant questions.......

 

Stages covered:

 

1. Simple transformations

 

2. Mapping attributes
