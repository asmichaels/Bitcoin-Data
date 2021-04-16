#DATA WRANGLING - aim is to compile all datasets into one dataframe: blockchain_data

library(tidyverse)
library(magrittr)

#data files
blockchain <- read_csv("blockchain100.csv") #source: my node and blockstream.info API

block_hashes <- read_csv("block_hashes.csv") #add to blockchain. having the               
#block hashes here will make further analysis easier
#source: my node

early_price <- read_csv("early price.csv") #source: blockchain.com

main_price <- read_csv("bitstamp.csv") 
#source:https://www.kaggle.com/mczielinski/bitcoin-historical-data

late_price <- read_csv("lateprice.csv", skip=1) 
#skip=1 as the first row is the url
#source: http://www.cryptodatadownload.com/data/bitstamp/

difficulty <- read_csv("difficulty.csv") #source: blockchain.com

hashrate <- read_csv("hashrate.csv") # source: blockchain.com


#first up: cleaning the data - blockchain and block hashes have an 
#irrelevant first column, as a legacy from python pandas -> csv
block_hashes_clean <- select(block_hashes, -X1)
blockchain_clean <- select(blockchain, -X1)

#binding these into a singular dataframe:
blockchain_data <- cbind(block_hashes_clean, blockchain_clean)


#we need to sort out time: for ease, we'll convert all files with 
#only timestamps to unix time.
UNIX_hash <- as.numeric(as.POSIXct(hashrate$Timestamp), tz = "GMT")
hashrate <- mutate(hashrate, UNIX_hash = UNIX_hash)

UNIX_difficulty <- as.numeric(as.POSIXct(difficulty$Timestamp), tz = "GMT")
difficulty <- mutate(difficulty, UNIX_diff = UNIX_difficulty)

UNIX_earlyprice <- as.numeric(as.POSIXct(early_price$Timestamp), tz = "GMT")
early_price <- mutate(early_price, UNIX_earlyprice = UNIX_earlyprice)


#the data: difficulty, hashrate and early price, have the same time stamps
#we'll mash them into one single dataframe

other_data_raw <- cbind(difficulty, hashrate, early_price)
other_data <- select(other_data_raw, UNIX_hash, difficulty, `hash-rate`, 
                     `market-price`)
other_data <- rename(other_data, UNIX = "UNIX_hash")

#next step: adding these variables to our main dataset
#I want to keep the block_time timestamps as they are in blockchain_data.
#I therefore need to take the timestamp from there, go into the other datasets
#and fetch the observation that is immediately before the block_time timestamp

#initializing empty column vectors where we put the data
difficulty_v <- c()
hashrate_v <- c()
early_price_v <- c()
main_price_v <- c()
late_price_v <- c()


#function below grabs the relevant data, and appends the empty vectors above
#the function takes a parameter, x, which is the block_time timestamp
other_data_fetch <- function(x) {
  unordered <- append(other_data$UNIX, x)
  ordered <- sort(unordered, decreasing = FALSE)
  index = which(ordered == x)
  difficulty_v <<- append(difficulty_v, other_data$difficulty[index - 1])
  hashrate_v <<- append(hashrate_v, other_data$`hash-rate`[index - 1])
  early_price_v <<- append(early_price_v, other_data$`market-price`[index - 1]) 
}

#running the for loop on the function
for (second in blockchain_data$Block_Time) {
  other_data_fetch(second)
}

#adding difficulty and hashrate data to blockchain_data
blockchain_data <- blockchain_data %>%
  mutate(Difficulty = difficulty_v)

blockchain_data <- blockchain_data %>%
  mutate(Hash_Rate = hashrate_v)


#taking only the first 1600 values from early_price_v
#this is because i want to use the more granular price data from the 
#main_price file
early_price_v1 <- early_price_v[1:1600]


#deleting all rows with missing values in main_price data
main_price_md <- main_price[complete.cases(main_price), ] 

#a function for grabbing price data, same idea as the function above
main_price_fetch <- function(x) {
  unordered <- append(main_price_md$Timestamp, x)
  ordered <- sort(unordered, decreasing = FALSE)
  index = which(ordered == x)
  #note: index[1] used below as occasionally block_time = main_price_md$timestamp
  #which causes 2 incidences of index
  main_price_v <<- append(main_price_v, main_price_md$Close[index[1] - 1]) 
}

#and the for loop to execute the function - WARNING: takes a while to 
#run this loop using the timestamps from rows 1601:6639 as this spans 
#the time of our main_price dataset
for (second in blockchain_data$Block_Time[1601:6639]) {
  main_price_fetch(second)
}

#the late price function below gets the most recent price data
late_price_fetch <- function(x) {
  unordered <- append(late_price$unix, x)
  ordered <- sort(unordered, decreasing = TRUE) 
  #decreasing=TRUE as the data is sorted from new -> old
  index = which(ordered == x)
  late_price_v <<- append(late_price_v, late_price$close[index[1] + 1]) 
  #index +1 as the list is upside down compared with before
}

for (second in blockchain_data$Block_Time[6640:6702]) {
  late_price_fetch(second)
}

#mashing all the vectors for price together:
price_v <- c()
price_v <- append(price_v, early_price_v1) %>%
  append(main_price_v) %>%
  append(late_price_v)

#adding the price vector to the main dataset:
blockchain_data <- blockchain_data %>%
  mutate(Price = price_v)


#adding another variable: number of block reward halving events
halvings_v <- c()

for (height in blockchain_data$Block_Height) {
  if (height < 210000){
    halvings_v <- halvings_v %>%
      append(0)
  }
  else if (height >= 210000 & height < 420000) {
    halvings_v <- halvings_v %>%
      append(1)
  }
  else if (height >= 420000 & height < 630000) {
    halvings_v <- halvings_v %>%
      append(2)
  }
  else if (height >= 630000) {
    halvings_v <- halvings_v %>%
      append(3)
  }
}

#adding halvings to blockchain data
blockchain_data <- blockchain_data %>%
  mutate(`Number of Halvings` = halvings_v)

#correcting an naming error by removing spaces
blockchain_data <- rename(blockchain_data, 
                          Number_of_Halvings = "Number of Halvings")

#adding a readable timestamp to the main dataset
blockchain_data <- blockchain_data %>%
  rename(Block_Time_UNIX = "Block_Time")

Block_Time <- as.POSIXct(blockchain_data$Block_Time_UNIX, 
                         origin="1970-01-01", tz="GMT")

blockchain_data <- blockchain_data %>%
  mutate(Block_Time = Block_Time)


#adding value in bitcoins (makes it easier to calculate miner revenue)
Value_BTC <- blockchain_data$Value/100000000

blockchain_data <- blockchain_data %>%
  mutate(Value_BTC = Value_BTC)

#renaming original value column
blockchain_data <- blockchain_data %>%
  rename(Value_Sat = "Value")


#getting miner revenue and adding it to the dataset
Miner_Revenue <- (blockchain_data$Value_BTC * blockchain_data$Price) %>%
  round(digits = 2)

blockchain_data <- blockchain_data %>%
  mutate(Miner_Revenue = Miner_Revenue)


#Getting block reward and adding it to the dataset
Block_Reward_BTC <- c(rep(50, 2100), rep(25, 2100), rep(12.5, 2100), 
                      rep(6.25, 402))

blockchain_data <- blockchain_data %>%
  mutate(Block_Reward_BTC = Block_Reward_BTC)


#getting value of transaction fees
Transaction_Fees <- (blockchain_data$Value_BTC - 
                       blockchain_data$Block_Reward_BTC)

blockchain_data <- blockchain_data %>%
  mutate(Transaction_Fees_BTC = Transaction_Fees)

#getting % of coinbase value that is transaction fees
Transaction_Fee_Percent <- ((blockchain_data$Value_BTC - 
                               blockchain_data$Block_Reward_BTC) * 
                              100 / blockchain_data$Value_BTC)

blockchain_data <- blockchain_data %>%
  mutate(Transaction_Fee_Percent = Transaction_Fee_Percent)

#writing to a csv file
write_csv(blockchain_data, "blockchain_data.2.csv")