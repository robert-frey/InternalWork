qu_db <- DBI::dbConnect(RSQLite::SQLite(),"QU_Database.sqlite")

FS <- DBI::dbGetQuery(qu_db,"SELECT * FROM FlightScope_Data")

FS <- FS %>% mutate(batter = stringr::str_squish(batter),
                    pitcher = stringr::str_squish(pitcher))

FS = FS %>% 
  mutate(pitch.spin.tilt = as.character(pitch.spin.tilt)) %>% mutate_at(vars(16:45),as.numeric)
FS_1008 = FS %>% mutate(date = format(as.Date(lubridate::parse_date_time(date, orders = c('m/d/Y', 'm/d/y', 'Y/m/d'))),"%Y-%m-%d"))

FS_SC_Data <- readr::read_csv2("**Insert FlightScope Scientific csv here**")

meters_to_feet <- function(column) {
  round(column * 3.281,3)
}

ms_to_mph <- function(column) {
  round(column * 2.237,3)
}

FS_SC_Data <- FS_SC_Data %>%
  separate(`Hit Ball StartPosition[m, m, m]`,c("hit.ball.start.pos.x","hit.ball.start.pos.y","hit.ball.start.pos.z"), sep = ",") %>%
  separate(`Pitch StrikeZone Position[m, m, m]`, c("pitch.strike.zone.pos.x","pitch.strike.zone.pos.y","pitch.strike.zone.pos.z"), sep = ",") %>%
  separate(`Hit Ball Landing Position[m, m, m]`,c("hc_x","hc_y","hc_z"), sep = ",") %>%
  separate(`Pitch Poly X[m, m, m, m, m]`,c("PPoly_x_1","PPoly_x_2","PPoly_x_3","PPoly_x_4","PPoly_x_5")) %>%
  separate(`Pitch Poly Y[m, m, m, m, m]`,c("PPoly_y_1","PPoly_y_2","PPoly_y_3","PPoly_y_4","PPoly_y_5")) %>%
  separate(`Pitch Poly Z[m, m, m, m, m]`,c("PPoly_z_1","PPoly_z_2","PPoly_z_3","PPoly_z_4","PPoly_z_5")) %>%
  select(hit.ball.start.pos.x:hit.ball.start.pos.z,pitch.strike.zone.pos.x:pitch.strike.zone.pos.z, hc_x:hc_z, `Hit Ball Carry Distance[m]`,`Hit Ball Speed[m/s]`, everything()) %>%
  dplyr::rename(hit.ball.speed = "Hit Ball Speed[m/s]",
                hit.carry.dist = "Hit Ball Carry Distance[m]",
                pitch.speed = "Pitch Speed[m/s]") %>%
  mutate_at(vars(hit.ball.start.pos.x:hit.carry.dist), as.numeric) %>%
  mutate_at(vars(hit.ball.start.pos.x:hit.carry.dist), meters_to_feet) %>%
  mutate(contact.point = hit.ball.start.pos.y - pitch.strike.zone.pos.y,
         hit.ball.speed = as.numeric(hit.ball.speed),
         pitch.speed = as.numeric(pitch.speed),
         contact.point.zone = case_when(contact.point >= 2 & hit.ball.start.pos.x <= 0 ~ 1,
                                        contact.point >= 2 & hit.ball.start.pos.x >= 0 ~ 2,
                                        contact.point >= 1 & contact.point < 2 & hit.ball.start.pos.x <= 0 ~ 3,
                                        contact.point >= 1 & contact.point < 2 & hit.ball.start.pos.x >= 0 ~ 4,
                                        contact.point >= 0 & contact.point < 1 & hit.ball.start.pos.x <= 0 ~ 5,
                                        contact.point >= 0 & contact.point < 1 & hit.ball.start.pos.x >= 0 ~ 6,
                                        contact.point < 0 & hit.ball.start.pos.x <= 0 ~ 7,
                                        contact.point < 0 & hit.ball.start.pos.x >= 0 ~ 8,
                                        TRUE ~ NA_real_)) %>%
  select(contact.point,everything())

FS_SC_Data <- FS_SC_Data %>% 
  dplyr::rename(num.runners = `BP-Base`,
                balls = `BP-Balls`,
                strikes = `BP-Strikes`,
                outs = `BP-Outs`,
                pfxx = "pfxx [m]",
                pfxz = "pfxz [m]",
                px = "px [m]",
                pz = "pz [m]",
                pitch.type = "Pitch Classification") %>%
  dplyr::mutate(count = paste0(balls,"-",strikes),
                pfxx = as.numeric(pfxx),
                pfxx = meters_to_feet(pfxx),
                pfxz = as.numeric(pfxz),
                pfxz = meters_to_feet(pfxz),
                px = as.numeric(px),
                px = meters_to_feet(px),
                pz = as.numeric(pz),
                pz = meters_to_feet(pz),
                hit.ball.speed = ms_to_mph(hit.ball.speed),
                pitch.speed = ifelse(pitch.speed > 110, pitch.speed/1000,pitch.speed),
                pitch.speed = ms_to_mph(pitch.speed),
                top.bot = ifelse("AP-InningIsTop"==1,"Top","Bot"),
                pitch.spin.tilt = paste0(`Pitch Spin Tilt Hours`,":",`Pitch Spin Tilt Minutes`,":00"),
                pitch.release.height = NA,
                pitch.release.side = NA,
                pitch.extension = meters_to_feet(as.numeric(`Pitch Extension Distance[m]`)),
                pitch.break.h = meters_to_feet(as.numeric(`Pitch Break (H)[m]`)),
                pitch.break.v = meters_to_feet(as.numeric(`Pitch Break (V)[m]`)),
                pitch.break.ind.v = meters_to_feet(as.numeric(`Pitch Break Ind (V)[m]`)),
                pitch.k.zone.height = NA,
                pitch.k.zone.offset = NA,
                pitch.zone.speed = meters_to_feet(as.numeric(`Approach Speed [m/s]`)),
                pitch.approach.v = meters_to_feet(as.numeric(`Approach Angle (V) [?]`)),
                pitch.approach.h = meters_to_feet(as.numeric(`Approach Angle (H) [?]`)),
                pitch.time = `Pitch FlightTime[s]`,
                home.team = NA,
                away.team = NA,
                pitcher.team = NA,
                batter.team = NA,
                Supervisor = NA,
                no = row_number(),
                pa.of.inning = 0,
                pitch.of.pa = 0,
                bauer.units = round(as.numeric(`Pitch Spin[rpm]`)/pitch.speed,1)) %>%
  select(guid = GUID,
         contact.point,
         hit.ball.start.pos.x,
         hc_x,
         hc_y,
         num.runners,
         balls,
         strikes,
         outs,
         count)

FS_1008 <- left_join(FS_1008,FS_SC_Data,by="guid")

FS = FS %>% 
  mutate(pitch.spin.tilt = as.character(pitch.spin.tilt)) %>% mutate_at(vars(16:45),as.numeric)
FS = FS %>% mutate(date = format(as.Date(lubridate::parse_date_time(date, orders = c('m/d/Y', 'm/d/y', 'Y/m/d'))),"%Y-%m-%d"))

FS = rbind(FS %>% dplyr::select(-avg_h_mov,-avg_spin,-avg_v_mov,-avg_velo,
                                -h_mov_diff,-spin_diff,-stuff_plus,-v_mov_diff,-velo_diff,-mean_miss_distance,-mean_px,-mean_pz,-miss_distance, -location_plus),FS_1008)


### Calculation for Stuff+ and Location+ Start Here
whiffs <- c("Swinging Strike","Swinging Strikeout")

fouls <- c("Foul Ball")

called_s <- c("Called Strike","Foul Ball","Called Strikeout")

out <- c("GIDP","Contact Out","Fielders Choice")

good_results <- c(whiffs,fouls,called_s,out)

location_plus_table <- FS %>% dplyr::filter(!is.na(px),!is.na(pz),
                                            !is.na(pitch.type),
                                            pitch.call %in% good_results | hit.ball.speed <= 75) %>%
  
  dplyr::group_by(pitch.type,pitcher.hand) %>%
  dplyr::summarise(mean_px = median(px,na.rm=T),
                   mean_pz = median(pz,na.rm=T))

miss_distance_table <- left_join(FS %>% select(guid,pitcher.hand,pitch.type,px,pz),location_plus_table,by=c("pitch.type","pitcher.hand")) %>% 
  mutate(mean_miss_distance = sqrt((((((px*12)-(mean_px*12)))^2)+((((pz*12)-(mean_pz*12)))^2)))) %>% 
  group_by(pitcher.hand,pitch.type) %>%
  summarise(mean_miss_distance = mean(mean_miss_distance,na.rm=T),
            mean_px = median(mean_px,na.rm=T),
            mean_pz = median(mean_pz,na.rm=T))


stuff_plus_table <- FS %>% dplyr::filter(!is.na(pitch.speed),!is.na(pitch.spin),!is.na(pfxx),!is.na(pfxz),
                                         !is.na(pitch.type),
                                         ) %>%
dplyr::group_by(pitcher.hand,pitch.type) %>% 
  dplyr::summarise(
    avg_velo = mean(pitch.speed,na.rm=T),
    avg_spin = mean(pitch.spin,na.rm=T),
    avg_h_mov = mean(abs(pfxx),na.rm=T),
    avg_v_mov = mean(abs(pfxz),na.rm=T))

FS <- left_join(FS,stuff_plus_table,by=c("pitcher.hand","pitch.type"))

FS <- FS %>% 
  dplyr::mutate(
    velo_diff = round((((pitch.speed/avg_velo)-1)*100),2),
    spin_diff = round((((pitch.spin/avg_spin)-1)*100),2),
    h_mov_diff = round((((abs(pfxx)/avg_h_mov)-1)*50),2),
    v_mov_diff = round((((abs(pfxz)/avg_v_mov)-1)*50),2),
    stuff_plus = 100 + velo_diff + spin_diff + h_mov_diff + v_mov_diff,
    stuff_plus = round(stuff_plus,0),
    stuff_plus = ifelse(stuff_plus < 0,0,stuff_plus))

FS <- left_join(FS,miss_distance_table,by=c("pitch.type","pitcher.hand"))
FS <- FS %>% mutate(miss_distance = sqrt((((((px*12)-(mean_px*12)))^2)+((((pz*12)-(mean_pz*12)))^2))))

#(((Miss Distance - Predicted Miss Distance) * -1) + Predicted Miss Distance) / Predicted Miss Distance
#(((Miss Distance - Predicted Miss Distance) * -1) + Predicted Miss Distance) / Predicted Miss Distance
FS <- FS %>% mutate(location_plus = (miss_distance-mean_miss_distance)/mean_miss_distance,
                    location_plus = (100 * location_plus) * -1,
                    location_plus = 100 + location_plus,
                    location_plus = round(location_plus))
