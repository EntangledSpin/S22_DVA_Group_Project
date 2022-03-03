import datetime

import yake
import json
import pandas as pd
import datetime
from db_core.database import Database
import os
import uuid

db = Database() #Database Object
sql_folder = os.path.join(os.path.abspath("."),"sql")

## Global Parameters
UPLOAD = True
expirement_id = uuid.uuid4()
print(expirement_id)
ep_ids_file_path = os.path.join(sql_folder,'episode_ids.sql')
ep_text_file_path = os.path.join(sql_folder,'episode_text.sql')
ep_keywords_file_path = os.path.join(sql_folder,'episode_keywords.sql')
dedup_list = [i for i in range(0, 1, .1)]
print(dedup_list)
# print (ep_ids_file_path)
kw_extractor = yake.KeywordExtractor() #Standard YAKE keyword extractor object


## Custom YAKE parameters


language = "en"
max_ngram_size = 2
deduplication_threshold = 0.8
deduplication_function = 'seqm'
windows_size = 1
num_of_keywords = 15
features = None
stop_words = None

## Custom Yake extractor object


custom_kw_extractor = yake.KeywordExtractor(lan=language,
                                            n=max_ngram_size,
                                            dedupLim=deduplication_threshold,
                                            dedupFunc=deduplication_function,
                                            top=num_of_keywords,
                                            features=features,
                                            stopwords=stop_words,
                                            windowsSize=windows_size,
                                            )


#### Quick Test Output####

text_1 = "Can ride like a girl beat the odds stacked against Ozzy films? The Melbourne cup is the biggest event on the " \
         "Australian horse racing calendar given that it's the only sporting event Australian workplaces pause for you" \
         " might even say it's the biggest sporting event in the country here in Oz its affectionately deemed the race " \
         "that stops the nation in 2015 29 year-old Michelle Payne Road in it for the second time pain comes from a " \
         "racing family. She's the youngest of 10 children who were all raised by their widowed father. Father played " \
         "by Sam Neill without spoiling the full story. It's one where as a female jockey pain came up against just about" \
         " everything a boys club determined not to let her in tragedy and injury when she met a horse with a similarly " \
         "troubled past. It seemed fate that she would ride that horse the prince of Penzance in the cup, but boy were the " \
         "odds against them. In fact The Bookies gave them one hundred to one with such a remarkable story to tell it was just" \
         " a matter of time before someone turned it into a movie. The screenplay was written by Andrew not hacks whole ridge and" \
         " a lace mccraty Jack Irish while Aussie actress Rachel Griffiths has taken the reins as director just like pains Journey " \
         "ride, like a girl really is an emotional roller coaster Teresa Palmer as pain gives an award worthy performance. She " \
         "brings out the strength and stubbornness that pain displayed For Better or For Worse when she was up against the wall " \
         "her face at times seem impossibly expressive and creating a character you can't help but root for Neil gives one of his " \
         "greatest performances as the father fighting his desire to keep his children safe while still supporting their dreams, but" \
         " it's TV pain playing himself who is sure to wow people with his scene-stealing performance Afflicted with Down Syndrome. " \
         "Stevie is the closest in age to Michelle and they share a most beautiful bond which shines in the film in his first ever role." \
         " He gives a seemingly season performance delivering jokes with impeccable timing and conveying more emotion when it's called " \
         "for Griffith says she's everything in her Arsenal to create the feeling of being a racing Insider. She builds on this right " \
         "through to the film's deafening tents and colorful climax ride, like a girl details the sexism and uphill battle that pain " \
         "faced and how through sheer hard work and determination. She was able to rise above it. All the story trucks along nicely and " \
         "features a few recognizable. Asia faces like Magnus urbanski as a gambling none and Mick Malloy as a Stony face trainer and some" \
         " great Aussie music. From the likes of Amy shark. It's a feel good Against All Odds story that undoubtedly deserves to be heard " \
         "strap in for this funny heartwarming absolute Prize winner see it. If you're a champion of the underdog or you love a film full of " \
         "feels skip it if your auntie horse racing or you hate Ozzy accents, it gets four and a half stars out of five. Jump on over to the " \
         "seed or skip it Instagram page to let us know what you thought about this review and the film."

keywords_1 = custom_kw_extractor.extract_keywords(text_1)

text_2 = "Hello everybody and welcome all thanks to LD mobile. This is MBL over time after a huge week 9 1 were the Sydney Kings again reign supreme, the Cairns taipans in my mind at least become the most frustrating team in the league and Melbourne, you're not around zippered to on the road that ended a six-game winning streak plenty get into hashtag MBL over time. They're getting involved do it right now Adam BL has tagged see incredible and Night, I am surrounded by news Breakers going to start with Liam Santa Maria started judges now live every other company while I live. Hello can sure Livingston Jory. I'm a so William strings to your bow, man. Welcome to you. What's going on? I'll tell you what me alongside about 55,000 people fell off their chairs off. I see they came on Friday night. Yes, your co-host. Oh come on Santa Mon Ami one of them by surprise. What it was a huge thing, like like that shown the reason the only ones who get to that as the show does progress but let's look back on week 9 and starting with a game where the Cairns taipans welcome Melbourne Melbourne. You're not in a were brilliant Cameron Oliver 26 and 13 Scotty Machado. Your man was again three and straight double doubles are a good fun team to watch and homicide. We are going to bring this little bit more and they are a legitimate. Top four chance. Yeah, they look. I was just talking to Liam a couple of days ago about had they won the game versus illawarra. Where would they be on the ladder right now and they would definitely be fighting for top four spot. So great work by them both, especially they lead from the front. I mean right now is seeing highlights of illawarra versus New Zealand the dynamic those two Dynamic next stars go at it again and Him talk me through this. It was a lot of fun. But sick Henry was the man who won the game stepped up in Cory Webster and Scotty Hopson zaps 25 and some clutch defensive players back-to-back triple-doubles philomela ball. Of course, they want to talk frustrating teams. Yeah. Let's talk about those Brisbane bullies from the Brisbane Island. Do you get off sending smoke signals? I need someone to come pick me up because another Other 60-point first half and a loss and you know a couple that with a really impressive performance from the south east Melbourne Phoenix their first when outside of Melbourne for the franchise and second win on the road and this one of course was the big one on Sunday Sydney were unbelievable in that first half offensively in Melbourne remarkably for a team is stacked as Melbourne are they just had no answers. Yeah. They definitely had no answers Casper Ware. Let that thing from front from the start to the Finish. I mean regardless of what the score was. It was a masterful performance by the Sydney Kings and this game we're going to get into it masterful performance. Oh my God, shut up the Daniel Johnson 27 and career high rebound 1728 was a clinic it looked like he was in practice. We're getting into more of that later. Did we look we look so easy for him. So easy just Catch and shoot knocking. Yeah, that was that was just a flat-out ass whooping call it. What it was leads us into the first part of MBL overtime tonight who has been or who was the most impressive South is Melbourne Phoenix double digits down win by double digits. They are outstanding cans Peter and Melbourne United all the Adelaide 36 hours and I'm going to put my foot down here. I'm going to say the Adelaide 36 is going to the jungle and doing it was the most impressive win of the weekend. We all agree. It was very good. It was it was yes. It was very good. I'm going to tip my hat off to the southeast Melvin feet. Okay, they lost three in a row. This team has got there. They got their asses whooped in Perth as well. So we did not know what we were going to see for them to bounce back the way they did. They were down 16 and at third quarter and came back. We did the game. Right right and came back locked up defensively and 1 by 16 Kyle a No, oh my goodness. This is a quality pick up for him championship winning experience goes back to his roots and puts on that Jersey behind you Liam and you have to give credit. He he stepped up big you have to give credit to Ben magic. Oh my God, we've been waiting for this slump for him shooting-wise from Beyond The Arc to disappear and he had a great game Mitch Creek was Miss Creek defensively, but all in all great effort by the southeast Melvin feeling they've been fabulous snapped a three-game losing streak God. Went outside of Melbourne tick that off and in big news for the south east Melbourne Phoenix what you got? They are welcoming tie Wesley back what this week's oh my goodness. A grown man is on Target and I just needs to take off one last fitness test later in the week. I'm told and for their matchup against the defending champs at home in versus Nick K. Let's go be grown. Man's going to be back. So that Bells Of course the end of Jake Rockets time with South East Melbourne who has been really really good. Yes, so he's now floating around it'll be too soon to see whether someone snaps him up. Maybe illawarra. Oh, but maybe maybe but great to have the grown man back on hard work great work Jake rocket. I'm going to take it for Adelaide and not stick and a half of them. But I think they win on Sunday was masterful you mentioned that and while the end very often we say how the game wasn't that much of a difference. It really was and I should have won by 35 lat That's what the dominant performance was. This is attained that obviously Jerome Randall's play this cut all these guys ready Jamie late. So there's going to be a little teething issue and Joe Wright does such a wonderful job of us against the world. They were very unlucky not to beat Sydney and over time a couple of weeks ago in Adelaide a tip-in away from Adobe the and Brisbane the same thing they went to Melbourne and really probably the only team to outwork Seth is Melbourne Phoenix. They were dominant from go to wallow in that particular game. They continue to build and they were outstanding on the end and Daniel Johnson, you know, it's all good frustration. If we know is all-nba first team all-nba second team. We know where this guy's at when it comes to his top performance if he's able to put those type of performance as up we're talking about him in a Boomers situation more so than just being a linchpin of that Adelaide 36 esteem. Randall's being great and Griffin coming off. The bench has been a master stroke have no idea if he enjoys it or not, but he's playing a hell of a lot better offensively. It works better for him and the team and someone like a Daniel Dylan T's coming. Off the Bench old starting and teases aspect they are coming on all cylinders couple of quick points on that Daniel Johnson is the Dirk Nowitzki of the MBL you see it with the way plays and is turn around jumpers and is just efficiency from the floor and the fact that for such a long period of time the man's just got it done. Yes. What he needs is the championship that Dirk got that that Daniel Johnson doesn't quite have now you talk about that Master stroke bringing Eric Griffin Off the Bench, you know, what has enabled that that is the play of OB Chateau Beach a and a great job by Daniel Joy right finding that diamond in the rough while we shout out all these teams. Let's not let the Cairns taipans to slide underneath most frustrating team in the league. I raised the last week and I'll stand by three things going on. It's the best rated and I'm thinking that I do at all. Okay, they need a bead some of the team's blown write the velocity. That's why they're frustrating a hawks. Yeah, but then they desire Let's say it's all they fun to watch. You don't want to know existed on our expectations. I said here at the start of the year and gave them their own tear the bottom of the stand. So homicide while we finish last and we were sitting you're saying they're not going to win thinking less games that last year last year. They won six. They've got your five already read a gentlement top four contenders. And that was a really really impressive win over Melbourne. And now the job is to piece a few of these winds together and Scott the way they put that team together we speak about it week after week because it's been so impressive. Bolivar breaking out of that slump. He has been outstanding Scott Machado one of the best point guard got a question, please. If and this is a big if somehow cans sneak into that fourth spot and go to the finals. Would Scott Machado be the MVP League? He'll be in the conversation. I Daddy's in the camaraderie that I also think be in the car when it is why I'm frustrated because I think their best which was seen at different times and not just at home what they did in Perth against the Wildcats early in the year. It is a legitimate Final 14. That's my frustration around the Cairns taipans. They're incredibly fun to watch they are knuckling down and playing better defensively this year than they did last year is a group and a couple of changes here, but that's why I'm frustrated because you know, he's really from because I think it can stipends in the finals is would be glad cuddly you need some action and which autotest will play action bro. Haha. Even Aslan that Liam slit in the studs and done over the course of the weekend as well anyway, My while they were teams that are impressive. Let's get to two teams that were bitterly disappointing cans smacked around Melbourne. You're not a day when zip into and Perth Wildcats, which we just aren't accustomed to them doing what they did at home and they've done it a couple of times this year on Sunday afternoon who you more disappointed in United or the Wildcats. I think I'm not more disappointed in United. They were on the road playing two top teams. So they lost those that's cool but to be home Rested after a week of rest, you lose two cans and the way you lost the cans was embarrassing. Alright, so you got a whole week of tough practices Gleason for shows have an individual meetings with players. You have to come out and make a statement and prove a point and for them to lay an egg the way they did and we're just flat-out just it was a masterful performance all the way around Daniel Johnson literally again, kick their asses. That's You did Jerome Randall set the table 20 and 8 those type of numbers from those positions should never happen at home in a statement game and the way they went about it. That's just I mean it was a disgrace for me the I put out a video usually I go after them and I'm loud with all the theatrics. I was dead serious with what I said in that video and I can you save that stuff for him be alive it so I'm gonna out of it. Hold on. I know they give you more heat, right? That's right. Who's more disappointed Melbourne or Perth? Yeah, Brisbane. Oh, that's we could talk about all of them to blow us with you. What is going on with the bullets pull it together. I've been backing you guys for self-interest and there's a lot of guys on that team lacking in confidence. There's a logjam of players on Wings there I've been pulled from I believe it on dry land - I believe in the idea of okay, let's bring in these group of really talented players, maybe the positional fits not perfect. But they all our team guys with tremendous buy and we're going to make it work then not making it work the five and seven is still right there. They've got the same record as the taipans. Are we roll up and about in our feelings towards but they just don't look like a playoff team on Fortunately, and I think they have been super frustrating hashtag MBL over time to get involved at MBL disappointing or otherwise, let us know right now. We'll get to your questions very shortly. Of course. We've got him BL double overtime as well. We sometimes don't fit some stuff in so always check Wednesday as well at MBL. All right talking about the jungle RSC Arena and how dominant the Perth Wildcats have been already twice this year. We have seen them be really horrible Cairns taipans earlier in the year and again on Friday against the Adelaide 36 as well. It doesn't get any easier. Is it Sydney Kings Head to town and Boys that's going to be a classic two teams that have played some big games in the past. But as we have a look at this graphic at the Sydney Kings have been exceptionally good in Perth, but the question is does it still the title go through the jungle homicide? Well, I will see this. We will not I will not sit here and disrespect the defending champion because I know at the end they will be there at the end and finals and they were all different team because has of the experience that they have however Last week intense. They just got mopped off the floor couple days ago. They got their asses with next weekend. They're gonna get destroyed Kings is going to destroy him. There's nobody they are outmatched in every position, but the two positions just on this but the two position they are outmatched at every position point guard Casper got that to go. Cotton got that three, man. Sydney got that whoever they want to start DD really newly. They got that Tirico hand messed up he in a hundred percent for position. You got the best powerful within the league Jay Sean Tate versus a tired power forward. Okay, and after five is no way bro gets not going to outplay hunt. These guys ain't showing up. That's why I don't know your name show up just know it man. Just on that one was just on the screen. It's the unveils version of the Vinci Code if you work your way through it, so thank you two heads. Going Neville all I'm Bill votes to whoever read the tire essentially what the main point out of it last year person lost two games for the entire year one by one point the other one by two already have lost I think by 22 points and 11 points this year in the air and nearly does is a defense is that the team not working is a notary Co white of course, what is the main in your idea? What's Rico? I played that game right came off the bench still plan to call Sharon minutes, but it is it's not another Tirico while he's working his way back in I said it last week they are. Our the way that seems put together their fuck their margin for error is Tiny and they need Bryce continent to Rico why they need the whole system working perfectly in their half-court execution offensively, but no it's their defense. They ranked seventh in defensive efficiency on the season. They're one of the worst defensive rebounding teams in the league. They're not forcing turnovers, which their defense used to do or traditionally has done which has sparked transition game transition opportunities, and that's when you used to see their Imports out and throwing down started with, you know, James Ennis and And pray the right the right. All right and last year tariqa what Bryce cotton I had in transition because they're forcing terms not doing that. I think I need to start Mitch Norton. It's time to make that change and bring them in Martin Off the Bench just switch that a little bit Norton's minutes bump up a little bit Martin's come down get so that he can be more effective get after it defensively while he's on the floor because he doesn't feel like he has to play that bulk load of minutes, but no they're Jungle with them playing like this is no longer as scary as that used to beat Kudos Bank Arena is the hardest place to win right now speaking of you got something. Yeah, and guess what last year with all those things. You just mentioned who's gone from that team? Angus Brandt you're not just scoring on him in the post. Right? Who else is gone the heart and soul of that team Greg. Hyah. They don't have anybody coming off their bench anywhere near that. That's where you missing all that Ramon Off the Bench to bring that and you know, what else is going to be happening in that game speaking of the grown man. Come back my understanding the Kevin leash is about to make his return to action are and I'll tell you it is they've waited long enough. So he he's not coming back half-ass. He's coming back a hundred percent and you know what? That's the most dynamic the best backcourt in the league Kevin Lynch Casper Ware. To a to you want to lock up on the I need some rest let you take cotton for a little bit. All right, let me get some rest listen get a rings to the king. So the so the Wildcats take on the Kings then they come down and take on South East Melbourne with tog Wesley back in the lineup big weekend for the pig we can put Nick K. I tell you that has taken me all over time to get evolve. What do you got there? What was that teacher even sitting there? This is how I got this sent down to me Hoops Capital. Yes become. The bad well the Hoops the capital of basketball in Australia is Melbourne. It's enabling people of the world. Everybody knows that except Paul Smith. The owner who has tried to Trump up the idea that the that Sydney that also now Sydney as the wavy Ryan basic so they made up these t-shirts and I understand that cam got wined and dined at Kudos Bank in a recently when exactly say wined and dined I got Give this to you because you said you were going to wear it on the show. That's what I was gonna wear this shirt on the show. You just what I said, this is what I said. Well don't over the car when I said no and I said I said the Paul Smith I said you make the Grand Final you hosted game one. They've got a bar right next to Kudos Bank Arena. It's Brandon Sydney Kings. We will take MBL over time on the road at his expense and I will wear it. I'm the game Grand final game. Number one now. They're trying to change the goal post a little bit. So Me thinking. Okay. How many games do you think legitimately this Sydney Kings team? Can we ever spoke about the fact that even in the regular season pick regulars that you give me a number? Give me a number and I'll agree to it 27. I'll allow them one maybe two more loss 23 when this is like that 23rd getting a higher than 20 so that they won't go twenty three and five. They've been in twenty three and five. There's added a boomer 24. All right, they win 20 when they were in their 24th game. I will wear this on the show. Okay over or without the cardi. I'm actually probably going to cut the sleeves off. Like an old school college mustn't shoot. Hey speaking, of course not. I don't know if we have it out the back. He also sent me something else, but us can get you a picture of that. We don't have it. Ok. Well, we don't have it. I know exactly what that is, and I've got bashed at all right quickly next ours. We got to talk next stars because Jonathan Giovanni and Draft Express released this morning latest mock predictions projections for the 2020 NBA draft the Mellow ball at 1 RJ Hampton at five Josh green and we can't forget about the Australian at 16, but boys. We're not overly surprised by this but it just continued validation. These guys are going really well. I'm going to see this. I've said a lot you have I'm going to start off with Couple of weeks ago we were here. Liam said I'd like to see him get a couple triple-doubles of something they did. Check and check tick alright, um, 3211 and 1325 12 and 10 and when they have him at number one who else at the point guard position could put up numbers like that back-to-back in a 40-minute game. I don't know if I think the closest guy may have been Cedric Jackson. I don't know if he went back-to-back triple-doubles. Am I know they didn't have it? So this is the first time in the history in a 40-minute game in the NBL this young man did that is he worthy for you guys as a number one pick yet? He's definitely worthy. I'm not saying I'm not saying he's definitely the number one pick. I'll tell you this. I don't know about you Liam. He was a 10 times better the week before I'm not saying he wasn't impressive on the weekend. But I also don't think he was as great as its but this stats there's too much flesh that's in sport wholeheartedly and the first day in the history. Yeah. No 40-minute game in Australia joined to the go back to back on the trying to diminish how good these guys? Okay. I'm not seeing you. I also think I reckon has they were six cars on the wake in the NBL had a better game than it minimum. I'm talking about even understands what I supposed to know for a number one. Yeah. I know. I'm not saying what I'm saying. He's not in it 100% That's what I'm talking is in the top five. I just not I wasn't as impressed as I just really love me better then lamellar ball. We I think they're of what he asked for. Well, not me. He did wait to be number one. What's missing from you? Well, I missing from him that you're seeing. I'm not saying: tis a great simply saying that I don't think we can sit here and be solidified as a number one pick here in December based on an incredible three months of basketball. Don't get me wrong. He is in the top five what he needs incredible fiber. No, I don't think it's he did. Well, he can't leave I said this a couple of weeks I going anyway, I hope not. So the fact is that defensively of course the game on the week is that I saw it. Whacking Russell Westbrook for that exact game for five years. He took a lot of bad shots. He didn't exactly play great defense unlike the week before he chased rebounds at different times. And when the game was on the line, he didn't get it done. Now. He's a ten-year-old kid. So it's got to be held into some type of perspective. But I also think that for the last five years has been whacking Russell Westbrook's were chasing triple-double and I've got a slight feeling about that in a weekend the week before opening was 20 times better and he was clutch and they got the job done. I'm not saying he's definitely number one pick and let's say he's definitely not he's in that conversation, but I don't think the last two weeks little fires. I was worried you could do a hamstring. You're losing your mind. I mean, I was impressive because once you see I've never met anybody I haven't seen a seasoned Pro you haven't see the season nothing were friends. I'm just not Tiffany. The thing the thing is, I mean, he's been super impressive and he's we spoke about last week his improvement over the course of the years been tremendous and Jonathan Cavani and the and Schmidt's and all of the draft. But in the end the GM's can see it and they know that he is a level of talent that is worthy of the number one. Pick Willie go number one. That's go. No, that's what it depends on who is up with that people have that pic and Michael Jordan at my going to hold that thought we're going to finish this conversation and MBL double overtime. Okay, because there's two big things coming firstly. But believes who big things yeah. One of them is Santa's watching and I called it because we know how big it is. And of course starts and does on nba.com daughter you right now, but there's been a slight tweak laying what the hell if you do a little bit of a different style to send is watching this week and we're going to start with my first appearance in said is watching and my inability to explain why Casey Prather has been out of the Melbourne lineup in recent times Casey Prather into the game being knighted missed the last two with backs bangers, would you Excuse me. Beck's bag is barely it is that it's an issue that they spread to get you on the I got one myself the next one. Of course our man over the country. Sorry. Sorry guys. I have to cut you guys off. I just got a text. That is a blockbuster signing. I don't know if it's true. But I'm gonna say it is because Mark Worthington just text me and said Perth Wildcats just signed Shaun Livingston what I'm just telling you what the word on the street is you actually cut them off. Yeah. Okay bad The guy was that mean the old I had a blockbuster. Could you give my trepidation? I've heard you nervous before but that level oh anything else. Yeah speaking of weather. Hmm. Did anyone notice him in the Adelaide? Yes, follow. Yeah. Did you hear Joe right explanation? No, here's what he was doing. Just being pretty. Talking trash she didn't do anything but talk trash that was his job. He'll probably get more games now because it worked for him on Sunday and speaking of guys checking off what they needed to do you get that job done and guess what? Hmm every game this season. He's attended in RAC Arena on a Perth of loss. It's all right. Yep. I got cans taipans game. You never used to be popular to be popular. But good on you mate. And lastly, of course Xavier Cooks. The massive signing is Sydney kings boy didn't things change with with his circumstances quickly. Gosh, your dad is assistant coach of the illawarra Hawks. Is there any hey save stay here play with us. Yeah, there is open one day then be all about right now. I want to go to Europe and see the world while I'm young Xavier. Welcome to the Sydney Kings. How excited are you to be join the club? I'm super excited just being for the last two and a half days like conch so I hold my excited right now. What a good? Yeah. Welcome to the Sydney King with red roses. Hex light look in his eye like it was nervous and then he wasn't exactly exude an excitement there at the Sydney Kings media girl last year, but huge for the Sydney Kings. All right. I have a feeling as a because at some point in the next couple of years will feature in the LG Mobile MBL top 10, but definitely not this week is not playing. Let's see who did rum - Don let's have some fun. Chirality mobile MBL top ten at number 10 Melo Trimble or an easy little stroll to the hole. but Oliver Twist the plot is Kim flat sluts that shot somebody send melamed telegram that you're not getting that off against space cam in at number ten on to number nine and my name is Tom Abercrombie and can't nobody stop me climbing up over the Hawks to hammer home the rock arriving from the ceiling with feeling for New Zealand at number 9 at eight Sean Long gets double team, but that don't mean a thing missing is A shot off the glass but he's there for the rebound Rim smash just a huge Slam against cans at number eight on to number seven and OB Shay gets the steal but it's about to get real. It's a super Dario dunk hunt and oves gonna bear the brunt as that shot gets pumped at number 7 at 6:00. If Sean Long's mr. Double-double, then Le mellow balls, mr. Triple-double. But this time it's like Henry's got him in trouble. Leaving the kid in the rubble a massive denial and he did it in style and that gets sick Henry in at number six on to number five in arriving by are for Sydney. It's Jay Sean taking you got to be kidding me Jason hitting vertical quicker than a helicopter on this Rim rocker as 24 gets up off the floor Jay Sean in at number 5 at number 4 its Melbourne getting bit in the snake pit as kawatte. Annoy tosses it up. Into the atmosphere but never fear when space cams near Oliver goes way up for the filthy stops at number 4. Look who's Breaking Free at number 3 Joe. Luau O'Toole attacking Andrew Bogut at the rack. And once you go up, there's no going back folks. You can hang that poster with blu-tack Luella to with no fear of the rating defensive player of the year. He's in at number 3 at number two, we're gonna need more blue tack because what I'm going on an Airborne attack after dribbling behind his back. Tom. Abercrombie may want to cover his eyes cuz the kid can surely fly mellow ball in at number 2 but act number one can mellow go back to back? No, let's talk about sex baby. Let's talk about steel and spree. Let's talk about all the good things happening for sick Henry. Let's talk about SEC Henry delivering the salt and pepper on a player. Remember at number one. On the NBL I'll be set at two NBA Bo who of course is the voiceover and loves the MBL. He might be the number one international fans are stands right now. Alright MBL double overtime Adam BL get to it tomorrow got plenty we missed out on but plenty of questions MBL news asking is on Twitter Crockett to Sean Taylor either of them a chance to end up it'll work. Yes, which one either but I understand the Allure Hawks have expressed some interest in to shawntel. All right. There you go. That's it. We got 5 seconds. So I nothing to say to you. Mr. News breaker. Shout out to short Livingston. Love your work man, seeing you soon and in vehicle over time, we'll be back a double over to Hunter. Yeah."
keywords_2 = custom_kw_extractor.extract_keywords(text_2)
text_3 = "Today I pulled the card get grounded. I think that's such a powerful reminder that especially if you're an empath or you're in the service industry your healer just highly sensitive. That's so important to consistently have a practice of grounding. Grounding the energy within yourself grounding the body grounding the mind and there's lots of different ways that we can do that for ourselves. We can meditate with the Palms facing down. Grounding down we can focus on the root chakra, which is at the base of the tailbone and represented by the color red. We can focus our energy and awareness here. And we can also surround ourselves with color therapy. So wearing red adding some red into your home or your space. Connecting with that color surrounding yourself with it. We can also go outside and Earth putting our bare feet in the soil walking in the grass. If its cold where you are hugging a tree. Getting outside in nature going for a hike. I'm just closing her eyes and repeating a few mantras. I am safe. I am grounded. I am rooted therefore I Rise. So important to consistently be making that a practice and remembering that when you're feeling feelings of fear insecurity lock those all stem from an imbalance in the root chakra and that's your cue to get grounded to find a practice that works for you that grounds that energy down and protects that sacred energy inside of yourself and brings you back down. into feelings of being safe secure within yourself and with others Let's go ahead and do a grounding meditation together now coming to a easy seated position The Palms facing down closing the eyes and bringing your energy and awareness to the root chakra. visualizing the color red focusing all of your awareness at the base of the tailbone. Take a deep breath in. Sending your breath all the way down the spine to the tailbone. And as you exhale. Bringing the breath back up the spine and out through the nostrils. envisioning a Red Lotus Flower at the base of the tailbone with each breath The Red Lotus is unfolding. petal by petal layer by layer Where you focus on this energy the more the red light begins to spread throughout the pelvis the abdomen. You can feel all of your energy rooting down connecting to the Earth. As you breathe into this red energy. every cell of your being is rooted safe and secure"
keywords_3 = custom_kw_extractor.extract_keywords(text_3)

for keyword in keywords_1:
    print(keyword)
print ('\n')
for keyword in keywords_2:
    print(keyword)
print ('\n')
for keyword in keywords_3:
    print(keyword)

######################################


#### Iterating through all episode ids

sql = db.read_sql_path(ep_ids_file_path)
episodes = db.execute_sql(sql_path=ep_ids_file_path,return_list=True) #extract episode id's from db

total_episodes = len(episodes)  # Completion tracker - total episodes
count = 0  # Completion tracker - completed count

for id in episodes:

    keyword_list = []

    keyword_dict = dict({'date':datetime.date.today(),
                         'expirement_uuid':expirement_id,
                         'episode_uri_id': id,
                         'algorithm': "YAKE",
                         'results': {"keywords":[]},
                         'parameters': {'lan': language,
                                        'dedupLim': deduplication_threshold,
                                        'dedupFunc': deduplication_function,
                                        'n': max_ngram_size,
                                        'top': num_of_keywords,
                                        'features':features,
                                        'stopwords':stop_words,
                                        'windowsSize':windows_size
                                        }
                         })

    sql_text = db.read_sql_path(ep_text_file_path)
    sql_text = sql_text.replace('REPLACEME_ID',id)

    # sql_keywords = db.read_sql_path(ep_keywords_file_path)
    # sql_keywords = sql_keywords.replace('REPLACEME_ID','68Eyz1s96eGvjbFATMOJyN')


    text = db.execute_sql(sql=sql_text, return_list=True)[0]  # May need to extend the db class method for return_item = True
    # keys = db.execute_sql(sql = sql_keywords,return_list=True)
    # print(keys)
    keywords = custom_kw_extractor.extract_keywords(text)

    for keyword in keywords:

        keyword_list.append(keyword[0])

    print(keyword_list)

    keyword_dict['results']['keywords'] = keyword_list
    print(keyword_dict)

    keyword_df = pd.DataFrame([keyword_dict])

    keyword_df['parameters'] = list(map(lambda x: json.dumps(x), keyword_df['parameters']))
    keyword_df['results'] = list(map(lambda x: json.dumps(x), keyword_df['results']))

    if UPLOAD:
        keyword_df.to_sql('keyword_extraction_expirements', index=False,
                          schema='datalake', con=db.engine, if_exists="append")


    count +=1
    remaining = total_episodes - count
    print(remaining)