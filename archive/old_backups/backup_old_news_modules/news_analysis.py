import json
import os
from pathlib import Path
import pandas as pd
from datetime import datetime
import re
from urllib.parse import urlparse
from typing import List, Dict, Any, Tuple, Optional

# Reuse the news source lists from test_news_source_analysis.py
alternative_news_sources = [
    "180grader.dk",
    "24nyt.dk",
    "arbejderen.dk",
    "denkorteavis.dk",
    "dkdox.tv",
    "document.dk",
    "folkets.dk",
    "frihedensstemme.dk",
    "indblik.dk",
    "konfront.dk",
    "kontrast.dk",
    "newspeek.info",
    "nordfront.dk",
    "piopio.dk",
    "redox.dk",
    "sameksistens.com",
    "solidaritet.dk",
    "psst-nyt.dk",
    "180grader.dk",
    "24nyt.dk",
    "arbejderen.dk",
    "dagensblaeser.net",
    "danmarksfriefjernsyn.dk",
    "denkorteavis.dk",
    "denuafhaengige.dk",
    "dkdox.tv",
    "document.dk",
    "folkets.dk",
    "freeobserver.org",
    "tv.frihedensstemme.dk",
    "frihedsbrevet.dk",
    "indblik.dk",
    "indblik.net",
    "konfront.dk",
    "kontrast.dk",
    "newspeek.info",
    "nordfront.dk",
    "piopio.dk",
    "redox.dk",
    "responsmedie.dk",
    "sameksistens.com",
    "solidaritet.dk",
    "aktuelltfokus.se",
    "arbetaren.se",
    "bubb.la",
    "bulletin.nu",
    "detgodasamhallet.com",
    "direktaktion.nu",
    "epochtimes.se",
    "exakt24.se",
    "feministisktperspektiv.se",
    "flamman.se",
    "folkungen.se",
    "friasidor.is",
    "friatider.se",
    "ledarsidorna.se",
    "nationalisten.se",
    "newsvoice.se",
    "nordfront.se",
    "nyadagbladet.se",
    "nyatider.nu",
    "nyheteridag.se",
    "nyhetsbyran.org",
    "proletaren.se",
    "svegot.se",
    "riks.se",
    "samnytt.se",
    "samtiden.nu",
    "tidningensyre.se",
    "vaken.se",
    "addendum.org",
    "allesroger.at",
    "alpenschau.com",
    "anschlaege.at",
    "auf1.tv",
    "contra-magazin.com",
    "info-direkt.eu",
    "kontrast.at",
    "moment.at",
    "mosaik-blog.at",
    "neuezeit.at",
    "report24.news",
    "tagesstimme.com",
    "unser-mitteleuropa.com",
    "unsere-zeitung.at",
    "unzensuriert.at",
    "volksstimme.at",
    "wochenblick.at",
    "zackzack.at",
    "zurzeit.at",
    "achgut.com",
    "akweb.de",
    "anonymousnews.org",
    "Anti-spiegel.ru",
    "antifainfoblatt.de",
    "blauenarzisse.de",
    "bnr.de",
    "compact-online.de",
    "der-rechte-rand.de",
    "dieunbestechlichen.com",
    "direkteaktion.org",
    "ef-magazin.de",
    "epochtimes.de",
    "extremnews.com",
    "free21.org",
    "freiewelt.net",
    "diefreiheitsliebe.de",
    "jacobin.de",
    "journalistenwatch.com",
    "jungefreiheit.de",
    "jungewelt.de",
    "jungle.world",
    "kenfm.de",
    "kla.tvde",
    "klassegegenklasse.org",
    "konkret-magazin.de",
    "kraut-zone.de",
    "lotta-magazin.de",
    "marx21.de",
    "missy-magazine.de",
    "mmnews.de",
    "multipolar-magazin.de",
    "nachdenkseiten.de",
    "nachrichtenspiegel.de",
    "neopresse.com",
    "nuoviso.tv",
    "opposition24.com",
    "perspektive-online.net",
    "philosophia-perennis.com",
    "pi-news.net",
    "politikstube.com",
    "pravda-tv.com",
    "redglobe.de",
    "reitschuster.de",
    "rf-news.de",
    "de.rt.com",
    "rubikon.news",
    "sezession.de",
    "unsere-zeit.de",
    "truth24.net",
    "zaronews.world",
    "zuerst.de",
]

mainstream_news_sources = [
    # Danish National Media
    # Danish Regional/Local Media
    "dr.dk",
    "tv2.dk",
    "politiken.dk",
    "berlingske.dk",
    "information.dk",
    "jyllands-posten.dk",
    "borsen.dk",
    "ekstrabladet.dk",
    "bt.dk",
    "kristeligt-dagblad.dk",
    "weekendavisen.dk",
    "finans.dk",
    "altinget.dk",
    "tv2ostjylland.dk",
    "dbrs.dk",
    "samsoposten.dk",
    "mediawatch.dk",
    "lokalnytkolding.dk",
    "hvidovreavis.dk",
    "sydmedier.dk",
    "folkebladetlemvig.dk",
    "nyborgavis.dk",
    "lokalnythjoerring.dk",
    "nordhavn-avis.dk",
    "lokalnytaalborg.dk",
    "midtvendsysselavis.dk",
    "Østvendsysselfolkeblad.dk",
    "lokalnytassens.dk",
    "koldingavisen.dk",
    "pingvinnyt.dk",
    "naernyt.dk",
    "fla.de",
    "hverdagsnyt.dk",
    "oestbirk-avis.dk",
    "hornsherredlokalavis.dk",
    "voreslokalavis.dk",
    "medtechnews.dk",
    "hjertingposten.dk",
    "nyheder.dk",
    "saebyavis.dk",
    "valavis.blogspot.com",
    "lyngposten.com",
    "aeroedagblad.dk",
    "vafo.dk",
    "kanalfrederikshavn.dk",
    "fodevarewatch.dk",
    "nb-okonomi.dk",
    "tv2kosmopol.dk",
    "ugebrev.dk",
    "lokalnytkoebenhavn.dk",
    "skivefolkeblad.dk",
    "lokalnytodense.dk",
    "vodskovavis.dk",
    "farsoeavis.dk",
    "frederiksbergliv.dk",
    "videnskab.dk",
    "dragoer-nyt.dk",
    "jyllandsavisen.dk",
    "tvmidtvest.dk",
    "a4medier.dk",
    "midtjyllandsavis.dk",
    "viborg-folkeblad.dk",
    "jyllands-posten.dk",
    "ib.dk",
    "lokalnytfredericia.dk",
    "skanderborg.lokalavisen.dk",
    "landbrugsavisen.dk",
    "lokalnytnyborg.dk",
    "folkebladet.info",
    "orestad-avis.dk",
    "netavisnord.dk",
    "standby.dk",
    "erhvervplus.dk",
    "olfi.dk",
    "zetland.dk",
    "kunmors.dk",
    "tvsyd.dk",
    "odenseavisen.dk",
    "radar.dk",
    "nibeavis.dk",
    "tv2bornholm.dk",
    "pov.international",
    "skanderborgliv.dk",
    "politiken.dk",
    "fanougeblad.dk",
    "herpaaoeen.dk",
    "information.dk",
    "kristeligt-dagblad.dk",
    "lokalavisen.dk",
    "mm.dk",
    "journalista.dk",
    "norddjurs.lokalavisen.dk",
    "fjordavisen.nu",
    "pro.ing.dk",
    "favrskov.lokalavisen.dk",
    "tidende.dk",
    "folketidende.dk",
    "ugeavisen.dk",
    "sn.dk",
    "lokalnytaarhus.dk",
    "billundonline.dk",
    "herningfolkeblad.dk",
    "tjekdet.dk",
    "ditfjends.dk",
    "galtenfolkeblad.dk",
    "rebildidag.dk",
    "dknyt.dk",
    "skagensavis.dk",
    "maarsletavis.dk",
    "vesthimmerlandsavis.dk",
    "struernyheder.dk",
    "cphpost.dk",
    "monitormedier.dk",
    "risbjergco.dk",
    "karrebaeksminde.dk",
    "netavisengrindsted.dk",
    "tv2fyn.dk",
    "dinavis.dk",
    "avisen.dk",
    "randersidag.dk",
    "ikast-brandenyt.dk",
    "regionsavisen.dk",
    "ditoverblik.dk",
    "softennyt.dk",
    "avisendanmark.dk",
    "ligeher.nu",
    "koldingnyheder.dk",
    "jammerbugtposten.dk",
    "netavisen.nu",
    "raeson.dk",
    "kjavis.dk",
    "dagbladet-holstebro-struer.dk",
    "netavisen-sjaelland.dk",
    "seoghoer.dk",
    "tv2nord.dk",
    "vejleavisen.dk",
    "brandebladet.dk",
    "mariagerfjordposten.dk",
    "jv.dk",
    "nordiskemedier.dk",
    "sonderborgnyt.dk",
    "finans.dk",
    "lokalnytvejle.dk",
    "maskinbladet.dk",
    "nykavis.dk",
    "weekendavisen.dk",
    "helsingordagblad.dk",
    "zand.news",
    "heleherlev.dk",
    "lokalnytkerteminde.dk",
    "lokalnytmiddelfart.dk",
    "aarhus.lokalavisen.dk",
    "lokalnytsvendborg.dk",
    "middelfartavisen.dk",
    "nordsoeposten.dk",
    "nordjyske.dk",
    "tv2east.dk",
    "version2.dk",
    "tv2.dk",
    "fyens.dk",
    "stiften.dk",
    "hsfo.dk",
    "jv.dk",
    "nordjyske.dk",
    "sn.dk",
    "amtsavisen.dk",
    "folkebladet.dk",
    "dagbladetringskjern.dk",
    "herningfolkeblad.dk",
    # Danish Online-Only News
    "mediawatch.dk",
    "journalisten.dk",
    "mm.dk",
    "ing.dk",
    "version2.dk",
    # International
    "bbc.com",
    "bbc.co.uk",
    "nytimes.com",
    "theguardian.com",
    "reuters.com",
    "cnn.com",
    "washingtonpost.com",
    "bloomberg.com",
    "economist.com",
    "BBC",
    "CNN",
    "Reuters",
    "aachener-nachrichten.de",
    "aachener-zeitung.de",
    "schwaebische.de",
    "wiesbadener-kurier.de",
    "bo.de",
    "bnn.de",
    "kreiszeitung.de",
    "weser-kurier.de",
    "wn.de",
    "augsburger-allgemeine.de",
    "aichacher-zeitung.de",
    "swp.de",
    "alfelder-zeitung.de",
    "waz-online.de",
    "allgaeuer-anzeigeblatt.de",
    "idowa.de",
    "azonline.de",
    "allgemeine-zeitung.de",
    "come-on.de",
    "giessener-allgemeine.de",
    "pnp.de",
    "az-online.de",
    "nordbayern.de",
    "mittelbayerische.de",
    "onetz.de",
    "nordkurier.de",
    "derpatriot.de",
    "harlinger.de",
    "svz.de",
    "aerztezeitung.de",
    "bz-berlin.de",
    "bkz.de",
    "nw.de",
    "westfalen-blatt.de",
    "rnz.de",
    "fnp.de",
    "badische-zeitung.de",
    "badisches-tagblatt.de",
    "shz.de",
    "infranken.de",
    "bayerische-staatszeitung.de",
    "bayernkurier.de",
    "berchtesgadener-anzeiger.de",
    "rundschau-online.de",
    "rp-online.de",
    "rhoenundsaalepost.de",
    "ostsee-zeitung.de",
    "morgenweb.de",
    "abendblatt-berlin.de",
    "berliner-kurier.de",
    "morgenpost.de",
    "berliner-zeitung.de",
    "noz.de",
    "bild.de",
    "szbz.de",
    "bbv-net.de",
    "boehme-zeitung.de",
    "borkenerzeitung.de",
    "boersen-zeitung.de",
    "mainpost.de",
    "main-echo.de",
    "maz-online.de",
    "braunschweiger-zeitung.de",
    "brv-zeitung.de",
    "boyens-medien.de",
    "allgaeuer-zeitung.de",
    "buerstaedter-zeitung.de",
    "butzbacher-zeitung.de",
    "staatsanzeiger.de",
    "cellesche-zeitung.de",
    "ovb-online.de",
    "evangelisch.de",
    "zeit.de",
    "cnv-medien.de",
    "merkur.de",
    "echo-online.de",
    "das-parlament.de",
    "24vest.de",
    "dewezet.de",
    "n-land.de",
    "freitag.de",
    "prignitzer.de",
    "tagesspiegel.de",
    "teckbote.de",
    "westallgaeuer-zeitung.de",
    "deutsche-handwerks-zeitung.de",
    "dvz.de",
    "die-glocke.de",
    "dieharke.de",
    "nq-online.de",
    "rheinpfalz.de",
    "die-tagespost.de",
    "welt.de",
    "da-imnetz.de",
    "mittelhessen.de",
    "lvz.de",
    "saechsische.de",
    "donaukurier.de",
    "dorstenerzeitung.de",
    "dnn.de",
    "dzonline.de",
    "gea.de",
    "kn-online.de",
    "goettinger-tageblatt.de",
    "einbecker-morgenpost.de",
    "ejz.de",
    "emderzeitung.de",
    "ev-online.de",
    "esslinger-zeitung.de",
    "express.de",
    "fehmarn24.de",
    "fla.de",
    "verlag-dreisbach.de",
    "hna.de",
    "wlz-online.de",
    "frankenpost.de",
    "faz.net",
    "fr.de",
    "flz.de",
    "fnweb.de",
    "freiepresse.de",
    "fuldaerzeitung.de",
    "gandersheimer-kreisblatt.de",
    "gaeubote.de",
    "gnz.de",
    "kreis-anzeiger.de",
    "general-anzeiger-bonn.de",
    "ga-online.de",
    "giessener-anzeiger.de",
    "gifhorner-rundschau.de",
    "gmuender-tagespost.de",
    "goslarsche.de",
    "gn-online.de",
    "muensterschezeitung.de",
    "haller-kreisblatt.de",
    "halternerzeitung.de",
    "abendblatt.de",
    "mopo.de",
    "hanauer.de",
    "handelsblatt.com",
    "haz.de",
    "harzkurier.de",
    "stimme.de",
    "hellwegeranzeiger.de",
    "helmstedter-nachrichten.de",
    "hersfelder-zeitung.de",
    "hildesheimer-allgemeine.de",
    "hurriyet.com.tr",
    "ivz-aktuell.de",
    "ikz-online.de",
    "juedische-allgemeine.de",
    "kevelaerer-blatt.de",
    "ksta.de",
    "kornwestheimer-zeitung.de",
    "krzbb.de",
    "lahrer-zeitung.de",
    "lampertheimer-zeitung.de",
    "szlz.de",
    "landeszeitung.de",
    "ln-online.de",
    "lr-online.de",
    "lauterbacher-anzeiger.de",
    "leinetal24.de",
    "leonberger-kreiszeitung.de",
    "lz.de",
    "lkz.de",
    "main-spitze.de",
    "marbacher-zeitung.de",
    "verlagshaus-jaumann.de",
    "moz.de",
    "medical-tribune.de",
    "insuedthueringen.de",
    "derwesten.de",
    "milligazete.com.tr",
    "mt.de",
    "mz-web.de",
    "tag24.de",
    "muehlacker-tagblatt.de",
    "muensterlandzeitung.de",
    "om-online.de",
    "mv-online.de",
    "murrhardter-zeitung.de",
    "maerkte-weltweit.de",
    "naumburger-tageblatt.de",
    "neckar-chronik.de",
    "ndz.de",
    "neuepresse.de",
    "np-coburg.de",
    "nrz.de",
    "nordbayerischer-kurier.de",
    "nnn.de",
    "nord24.de",
    "nwzonline.de",
    "ntz.de",
    "op-marburg.de",
    "oberhessische-zeitung.de",
    "obermain.de",
    "wnoz.de",
    "rhein-zeitung.de",
    "op-online.de",
    "oz-online.de",
    "otz.de",
    "paz-online.de",
    "peiner-nachrichten.de",
    "pz-news.de",
    "pirmasenser-zeitung.de",
    "remszeitung.de",
    "rga.de",
    "tagblatt.de",
    "rheiderland.de",
    "rheingau-echo.de",
    "ruhrnachrichten.de",
    "saarbruecker-zeitung.de",
    "salzgitter-zeitung.de",
    "sn-online.de",
    "schifferstadter-tagblatt.de",
    "zvw.de",
    "schwaebische-post.de",
    "schwarzwaelder-bote.de",
    "schwarzwaelder-post.de",
    "beobachter-online.de",
    "serbske-nowiny.de",
    "siegener-zeitung.de",
    "soester-anzeiger.de",
    "solinger-tageblatt.de",
    "tageblatt.de",
    "stuttgarter-nachrichten.de",
    "stuttgarter-zeitung.de",
    "sueddeutsche.de",
    "suedkurier.de",
    "tah.de",
    "taz.de",
    "thueringer-allgemeine.de",
    "tlz.de",
    "torgauerzeitung.com",
    "traunsteiner-tagblatt.de",
    "volksfreund.de",
    "tz.de",
    "uckermarkkurier.de",
    "usinger-anzeiger.de",
    "vkz.de",
    "vdi-nachrichten.com",
    "vesti-online.com",
    "vogtland-anzeiger.de",
    "volksstimme.de",
    "wz-net.de",
    "werra-rundschau.de",
    "wz.de",
    "wp.de",
    "wr.de",
    "wa.de",
    "wetterauer-zeitung.de",
    "lokal26.de",
    "wolfenbuetteler-zeitung.de",
    "wolfsburger-nachrichten.de",
    "wormser-zeitung.de",
    "yeniozgurpolitika.net",
    "zak.de",
    "spiegel.de",
    "focus.de",
    "stern.de",
    "wiwo.de",
    "manager-magazin.de",
    "abendzeitung-muenchen.de",
    "life-und-style.info",
    "myself.de",
    "ok-magazin.de",
    "superillu.de",
    "instyle.de",
    "wunderweib.de",
    "freizeitrevue.de",
    "glamour.de",
    "faces.ch",
    "3sat.de",
    "arte.tv",
    "bb-mv-lokaltv.de",
    "br.de",
    "brf.de",
    "daserste.de",
    "dw.com",
    "hr-fernsehen.de",
    "kika.de",
    "mdr.de",
    "ndr.de",
    "phoenix.de",
    "radiobremen.de",
    "rbb-online.de",
    "sr.de",
    "swrfernsehen.de",
    "ard.de",
    "wdr.de",
    "zdf.de",
    "deutschlandfunk.de",
    "deutschlandfunkkultur.de",
    "deutschlandradio.de",
    "deutschlandfunknova.de",
    "hr1.de",
    "hr2.de",
    "hr3.de",
    "hr4.de",
    "you-fm.de",
    "hr-inforadio.de",
    "n-joy.de",
    "antennebrandenburg.de",
    "fritz.de",
    "inforadio.de",
    "radioeins.de",
    "unserding.de",
    "dasding.de",
    "swr3.de",
    "wdrmaus.de",
    "1-2-3.tv",
    "anixehd.tv",
    "astrotv.de",
    "bibeltv.de",
    "bwfamily.tv",
    "channel21.de",
    "comedycentral.tv",
    "deluxemusic.tv",
    "deraktionaertv.de",
    "deutsches-musik-fernsehen.de",
    "disney.de",
    "dmax.de",
    "drf-tv.de",
    "eotv.de",
    "euronews.com",
    "eurosport.de",
    "ewtn.de",
    "www-health.tv",
    "hgtv.com",
    "hopechannel.de",
    "hse24.de",
    "juwelo.de",
    "k-tv.org",
    "kabeleins.de",
    "kabeleinsdoku.de",
    "mediashop.tv",
    "mtv.de",
    "n-tv.de",
    "nick.de",
    "nitro-tv.de",
    "pearl.de",
    "prosieben.de",
    "prosiebenmaxx.de",
    "qs24.tv",
    "qvc.de",
    "rictv.de",
    "rtl2.de",
    "rtlplus.de",
    "rtl.de",
    "sat1.de",
    "sixx.de",
    "sonnenklar.tv",
    "sport1.tv",
    "startv.com.tr",
    "superrtl.de",
    "tele5.de",
    "tlc.de",
    "toggo.de",
    "vox.de",
    "weltderwunder.de",
    "xite.tv",
    "sky.de",
    "13thstreet.de",
    "animalplanet.de",
    "auto-motor-und-sport.de",
    "aenetworks.de",
    "axn.com",
    "bongusto.de",
    "cartoonnetwork.de",
    "stingray.com",
    "discovery.com",
    "foxchannel.de",
    "geo-television.de",
    "goldstar-tv.de",
    "gutelaunetv.de",
    "heimatkanal.de",
    "history.com",
    "jukebox-tv.de",
    "junior-programme.de",
    "kabeleinsclassics.de",
    "kinowelt.tv",
    "lust-pur.tv",
    "marcopolo.de",
    "motorsport.tv",
    "motorvision.tv",
    "nationalgeographic.de",
    "natgeotv.com",
    "nauticalchannel.com",
    "nickjr.de",
    "planetradio.de",
    "prosiebenfun.de",
    "rck-tv.de",
    "rtl-crime.de",
    "rtl-living.de",
    "rtl-passion.de",
    "sat1emotions.de",
    "silverline24.de",
    "sonychannel.de",
    "syfy.de",
    "tnt-tv.de",
    "universaltv.de",
    "absolutradio.de",
    "energy.de",
    "erf.de",
    "klassikradio.de",
    "lulu.fm",
    "schlagerradiob2.de",
    "radiobob.de",
    "horeb.org",
    "schlagerparadies.de",
    "radioteddy.de",
    "rockantenne.de",
    "hitradio-rtl.de",
    "schwarzwaldradio.com",
    "sunshine-live.de",
    "antenne1.de",
    "bigfm.de",
    "egofm.de",
    "radio7.de",
    "regenbogen.de",
    "baden.fm",
    "dieneue1077.de",
    "die-neue-welle.de",
    "donau3fm.de",
    "hitradio-ohr.de",
    "neckaralblive.de",
    "dasneueradioseefunk.de",
    "radioton.de",
    "antenne.de",
    "megaradio.bayern",
    "rt1.de",
    "radio-augsburg.de",
    "fanatsy.de",
    "smartradio.de",
    "top-fm.de",
    "christlichesradio.de",
    "mk-online.de",
    "radio2day.de",
    "radioarabella.de",
    "charivari.de",
    "feierwerk.de",
    "radiogong.de",
    "camillo929.de",
    "hitradion1.de",
    "jazzstudio.de",
    "meinlieblingsradio.de",
    "n904beat.de",
    "radiopray.de",
    "aref.de",
    "radiof.de",
    "radio-meilensteine.de",
    "starfm.de",
    "gongfm.de",
    "radio-opera.de",
    "radio8.de",
    "radioawn.de",
    "radio-trausnitz.de",
    "unserradio.de",
    "bayernwelle.de",
    "isw.fm",
    "alpenwelle.de",
    "radio-in.de",
    "radio-oberland.de",
    "extra-radio.de",
    "radio-bamberg.de",
    "euroherz.de",
    "mainwelle.de",
    "radio-plassenburg.de",
    "ramasuri.de",
    "rsa-radio.de",
    "allgaeuhit.de",
    "radiohastagplus.de",
    "radioprimaton.de",
    "primavera24.de",
    "6rtl.com",
    "spreeradio.de",
    "radio-potsdam.de",
    "jam.fm",
    "rs2.de",
    "radio-cottbus.de",
    "bbradio.de",
    "kissfm.de",
    "berliner-rundfunk.de",
    "domradio.de",
    "lausitzwelle.de",
    "fluxfm.de",
    "hitradio-skw.de",
    "jazzradio.net",
    "radio.de",
    "pure-fm.de",
    "radiogold.de",
    "schlager.radio",
    "metropolfm.de",
    "rockland.de",
    "radio21.de",
    "80s80s.de",
    "917xfm.de",
    "hamburg-zwei.de",
    "radiohamburg.de",
    "harmonyfm.de",
    "ffh.de",
    "antennemv.de",
    "ostseewelle.de",
    "ffn.de",
    "meerradio.de",
    "radio38.de",
    "radio90vier.de",
    "radio-hannover.de",
    "radio-nordseewelle.de",
    "radioosnabrueck.de",
    "antenne-ac.de",
    "antennedueselldorf.de",
    "antennemuenster.de",
    "antenneniederrhein.de",
    "antenneunna.de",
    "hellwegradio.de",
    "radio901.de",
    "raidoberg.de",
    "radiobielefeld.de",
    "radiobochum.de",
    "radiobonn.de",
    "radioduisburg.de",
    "radioemscherlippe.de",
    "radioenneperuhr.de",
    "radioerft.de",
    "radioessen.de",
    "radioeuskirchen.de",
    "radioguetersloh.de",
    "radiohagen.de",
    "radioherford.de",
    "radioherne.de",
    "radiohochstift.de",
    "radiokiepenkerl.de",
    "radiokw.de",
    "radioleverkusen.de",
    "radiolippe.de",
    "lippewelle.de",
    "radiomk.de",
    "radiomuelheim.de",
    "radioneandertal.de",
    "radiooberhausen.de",
    "radiorsg.de",
    "radiorst.de",
    "radiorur.de",
    "radiosauerland.de",
    "radiosiegen.de",
    "radiovest.de",
    "radiowaf.de",
    "radiowestfalica.de",
    "radiowmw.de",
    "radiowuppertal.de",
    "welleniederrhein.de",
    "rpr1.de",
    "antenne-io.de",
    "antenne-kh.de",
    "antenne-kl.de",
    "antenne-koblenz.de",
    "antenne-landau.de",
    "antenne-mainz.de",
    "antenne-pirmasens.de",
    "antenne-zweibruecken.de",
    "cityradio-trier.de",
    "classicrock-radio.de",
    "cityradio-saarland.de",
    "salue.de",
    "apolloradio.de",
    "radiochemnitz.de",
    "radiodresden.de",
    "radioerzgebirge.de",
    "radiolausitz.de",
    "radioleipzig.de",
    "radiozwickau.de",
    "vogtlandradio.de",
    "secondradio.de",
    "radiobrocken.de",
    "radiosaw.de",
    "deltaradio.de",
    "rsh.de",
    "antenne-sylt.de",
    "antennethueringen.de",
    "landeswelle.de",
    "radiotop40.de",
    "bermudafunk.org",
    "radio-fds.de",
    "freies-radio.de",
    "freies-radio-wiesental.de",
    "querfunk.de",
    "rdl.de",
    "freefm.de",
    "sthoerfunk.de",
    "wueste-welle.de",
    "lora924.de",
    "radiomuenchen.net",
    "radio-z.net",
    "88vier.de",
    "kcrw.com",
    "medialabnord.de",
    "fsk-hh.org",
    "tidenet.de",
    "antennebergstrasse.de",
    "freies-radio-kassel.de",
    "radiodarmstadt.de",
    "radio-quer.de",
    "radio-rheinwelle.de",
    "radio-r.de",
    "radio-rum.de",
    "radiox.de",
    "rundfunk-meissner.org",
    "lohro.de",
    "nb-radiotreff.de",
    "radio98eins.de",
    "studio-malchin.de",
    "emsvechtewelle.de",
    "leinehertz.net",
    "oeins.de",
    "osradio.de",
    "radio-aktiv.de",
    "radio-jade.de",
    "radio-marabu.de",
    "okerwelle.de",
    "ostfriesland.de",
    "tonkuhle.de",
    "zusa.de",
    "stadtradio-goettingen.de",
    "antenne-bethel.de",
    "muenster.org",
    "coloradio.org",
    "radioblau.de",
    "radiocorax.de",
    "radio-hbw.de",
    "oksh.de",
    "radio-enno.de",
    "radio-frei.de",
    "tu-ilmenau.de",
    "radiolotte.de",
    "radio-okj.de",
    "srb.fm",
    "wartburgradio.org",
    "horads.de",
    "radioaktiv.org",
    "kit.edu",
    "maxneo.de",
    "funklust.de",
    "kanal-c.net",
    "couchfm.de",
    "bonn.fm",
    "campusfm.info",
    "ctdasradio.de",
    "eldoradio.de",
    "hertz879.de",
    "hochschulradio-aachen.de",
    "hochschulradio.de",
    "koelncampus.com",
    "triquency.de",
    "radius921.de",
    "radio-mittweida.de",
    "radiomephisto.de",
    "20min.ch",
    "friday-magazine.ch",
    "24heures.ch",
    "3plus.tv",
    "4plus.tv",
    "5plus.tv",
    "6plus.tv",
    "7radio.ch",
    "smartradio.ch",
    "aargauerzeitung.ch",
    "televox.ch",
    "fcsg.ch",
    "alf-tv.ch",
    "alpenlandtv.ch",
    "alpen-welle.ch",
    "altamega.ch",
    "annabelle.ch",
    "anzeiger-luzern.ch",
    "anzeigerverband-bucheggberg-wasseramt.ch",
    "anzeigergls.ch",
    "anzeigerinterlaken.ch",
    "anzeiger-kirchberg.ch",
    "anzeigerkonolfingen.ch",
    "anzeigermichelsamt.ch",
    "azoe.ch",
    "anzeigerbern.ch",
    "anzeiger-erlach.ch",
    "anzeigertgo.ch",
    "anzeigerverbandbern.ch",
    "anzeigervomrottal.ch",
    "archebdo.ch",
    "arcmusique.ch",
    "arcinfo.ch",
    "auftanken.TV",
    "zulu-media.net",
    "badenertagblatt.ch",
    "bantigerpost.ch",
    "baernerbaer.ch",
    "bazonline.ch",
    "beautyundlife.ch",
    "beobachter.ch",
    "bernerlandbote.ch",
    "berneroberlaender.ch",
    "bestvision.tv",
    "bielbienne.com",
    "bielertagblatt.ch",
    "bilan.ch",
    "handelszeitung.ch",
    "birsfelderanzeiger.ch",
    "bibo.ch",
    "blick.ch",
    "rjb.ch",
    "boostertv.ch",
    "bote.ch",
    "wohleranzeiger.ch",
    "brunni.ch",
    "bzbasel.ch",
    "bernerzeitung.ch",
    "langenthalertagblatt.ch",
    "canal29.ch",
    "canal9.ch",
    "canalalpha.ch",
    "multivideo.ch",
    "absolutenetworks.ch",
    "channel55.tv",
    "chiassotv.ch",
    "chtv.ch",
    "mediaprofil.ch",
    "energy.ch",
    "beviacom.ch",
    "coopzeitung.ch",
    "cdt.ch",
    "CountryRadio.ch",
    "cransmontana.ch",
    "dasmagazin.ch",
    "databaar.ch",
    "derbund.ch",
    "landanzeiger.ch",
    "landbote.ch",
    "azmedien.ch",
    "unter-emmentaler.ch",
    "diaspora-tv.ch",
    "die-neue-zeit-tv.ch",
    "woz.ch",
    "dieutv.com",
    "diisradio.ch",
    "dorfblitz.ch",
    "dorfheftli.ch",
    "dregion.ch",
    "drita.tv",
    "dukascopy.tv",
    "brandsarelive.com",
    "electroradio.fm",
    "engadinerpost.ch",
    "ibexmedia.ch",
    "ewgoms.ch",
    "femina.ch",
    "sonntag.ch",
    "fuw.ch",
    "radiofm1.ch",
    "frauenfelderwoche.ch",
    "freiburger-nachrichten.ch",
    "frequencebanane.ch",
    "fridolin.ch",
    "frutiglaender.ch",
    "startv.ch",
    "gds.fm",
    "effingermedien.ch",
    "generationfm.ch",
    "genevalatina.ch",
    "ghi.ch",
    "volketswilernachrichten.ch",
    "glattwerk.ch",
    "globalfm.ch",
    "glueckspost.ch",
    "grenchnertagblatt.ch",
    "gvfm.ch",
    "hockeyfanradio.ch",
    "hockeyradio-ticino.ch",
    "hoefner.ch",
    "hoengger.ch",
    "hotelradio.fm",
    "caffe.ch",
    "illustrazione.ch",
    "tvtservices.ch",
    "ipmusic.ch",
    "jamesfm.ch",
    "journaldemorges.ch",
    "lejds.ch",
    "jump-tv.ch",
    "jungfrauzeitung.ch",
    "kanal8610.org",
    "kfn-ag.ch",
    "ktfm.ch",
    "lokalinfo.ch",
    "lacote.ch",
    "lagazette.ch",
    "la-gazette.ch",
    "lagruyere.ch",
    "laliberte.ch",
    "laregion.ch",
    "laregione.ch",
    "latele.ch",
    "ladiesdrive.tv",
    "laupenanzeiger.ch",
    "lausannecites.ch",
    "lematin.ch",
    "lemessager.ch",
    "lenouvelliste.ch",
    "lqj.ch",
    "leregional.ch",
    "letemps.ch",
    "lemanbleu.ch",
    "leutv.ch",
    "lfmtv.ch",
    "liewo.li",
    "illustre.ch",
    "limmattalerzeitung.ch",
    "informatore.net",
    "linoradio.com",
    "linthzeitung.ch",
    "btv-kreuzlingen-steckborn.ch",
    "ossingen.tv",
    "loly.ch",
    "lounge-radio.ch",
    "luzernerzeitung.ch",
    "magicradio.ch",
    "marcandcoradio.com",
    "marchanzeiger.ch",
    "maxtv.ch",
    "maxxima.org",
    "tcr-media.com",
    "migrosmagazin.ch",
    "mkd-music.tv",
    "mtv.ch",
    "murtenbieter.ch",
    "musig24.tv",
    "muttenzeranzeiger.ch",
    "mysports.ch",
    "my105.ch",
    "all3media.com",
    "nfz.ch",
    "nzz.ch",
    "nick.ch",
    "niederaemter-anzeiger.ch",
    "nrtv.ch",
    "oberaargauer.ch",
    "oberbaselbieterzeitung.ch",
    "obersee-nachrichten.ch",
    "oberwiggertaler.ch",
    "oltnertagblatt.ch",
    "onefm.ch",
    "onetv.ch",
    "onedancefm.ch",
    "openbroadcast.ch",
    "kabelfernsehen.ch",
    "ne84.ch",
    "rsi.ch",
    "rtr.ch",
    "rts.ch",
    "srf.ch",
    "swissinfo.ch",
    "puls8.ch",
    "powerup.ch",
    "premiumshopping.tv",
    "pressetv.ch",
    "prosieben.ch",
    "radio1.ch",
    "radio20coeurs.net",
    "radio24.ch",
    "radiobeo.ch",
    "radio32.ch",
    "3fach.ch",
    "r3i.ch",
    "argovia.ch",
    "basilisk.ch",
    "radiobern1.ch",
    "radio-bollwerk.ch",
    "radiobus.fm",
    "canal3.ch",
    "radiocentral.ch",
    "radiochablais.ch",
    "radiocite.ch",
    "django.fm",
    "auborddeleau.radio",
    "eviva.ch",
    "radiofr.ch",
    "radiofd.ch",
    "radiogloria.ch",
    "grrif.ch",
    "radiogwen.ch",
    "radioinside.ch",
    "kaiseregg.ch",
    "kanalk.ch",
    "lafabrik.ch",
    "radiolac.ch",
    "lfm.ch",
    "erf.ch",
    "radiololfm.ch",
    "lora.ch",
    "radio-lozâ€°rn.ch",
    "radioluzernpop.ch",
    "radiomaria.ch",
    "radiomelody.ch",
    "radiomultikulti.ch",
    "radiomunot.ch",
    "neo1.ch",
    "mitternachtsruf.ch",
    "radioonyx.org",
    "radiopilatus.ch",
    "pinkradio.com",
    "radiopositive.ch",
    "rabe.ch",
    "radioradius.ch",
    "rasa.ch",
    "rhonefm.ch",
    "rro.ch",
    "rouge.com",
    "stadtfilter.ch",
    "suedostschweiz.ch",
    "radiosummernight.ch",
    "sunshine.ch",
    "radioswissclassic.ch",
    "radioswissjazz.ch",
    "radioswisspop.ch",
    "radiotell.ch",
    "radioticino.com",
    "radiotop.ch",
    "verticalradio.ch",
    "radiovolare.com",
    "radiovostok.ch",
    "radiox.ch",
    "radio.ch",
    "radio4tng.ch",
    "radiochico.ch",
    "radio-jazz.com",
    "radiologisch.ch",
    "radioreveil.ch",
    "radiosport.ch",
    "rapbeatsradio.com",
    "redlineradio.ch",
    "ref-gais.ch",
    "zueriost.ch",
    "regiotvplus.ch",
    "restorm.com",
    "rheinwelten.ch",
    "riehener-zeitung.ch",
    "rtvislam.com",
    "rundfunkpositiv.ch",
    "rundfunk.fm",
    "s1tv.ch",
    "sarganserlaender.ch",
    "sat1.ch",
    "bockonline.ch",
    "shf.ch",
    "shn.ch",
    "schweiz5.ch",
    "schweizamwochenende.ch",
    "schweizerfamilie.ch",
    "schweizerhockeyradio.ch",
    "schweizer-illustrierte.ch",
    "skuizz.com",
    "sowo.ch",
    "solothurnerzeitung.ch",
    "sonntagszeitung.ch",
    "radioluz.ch",
    "spoonradio.com",
    "tagblatt.ch",
    "stadtanzeiger-olten.ch",
    "stadt-anzeiger.ch",
    "deinsound.ch",
    "sunradio.ch",
    "surentaler.ch",
    "surprise.ngo",
    "swiss1.tv",
    "upstream-media.ch",
    "swissquote.ch",
    "syri.tv",
    "tagblattzuerich.ch",
    "tagesanzeiger.ch",
    "tele1.ch",
    "tele-d.ch",
    "telem1.ch",
    "tvo-online.ch",
    "toponline.ch",
    "telez.ch",
    "telebaern.ch",
    "telebasel.ch",
    "telebielingue.ch",
    "teleclub.ch",
    "telenapf.ch",
    "tele-saxon.ch",
    "teleswizz.ch",
    "teleticino.ch",
    "televersoix.ch",
    "televista.ch",
    "tep.ch",
    "tessinerzeitung.ch",
    "thuneramtsanzeiger.ch",
    "thunertagblatt.ch",
    "toxic.fm",
    "tdg.ch",
    "mynmz.ch",
    "tvoberwallis.tv",
    "tv-rheintal.ch",
    "telesuedostschweiz.ch",
    "tv24.ch",
    "tv25.ch",
    "tv4tng.ch",
    "tvm3.ch",
    "uristier.ch",
    "sevj.ch",
    "verniervisions.ch",
    "vibracionlatina.com",
    "radiovintage.ch",
    "virginradiohits.ch",
    "meteonews.ch",
    "wiggertaler.ch",
    "willisauerbote.ch",
    "wochenblatt.ch",
    "wochen-zeitung.ch",
    "zofingertagblatt.ch",
    "zugerpresse.ch",
    "zuonline.ch",
    "zsz.ch",
    "10tv.com",
    "6abc.com",
    "abc13.com",
    "abc15.com",
    "abc7.com",
    "abc7chicago.com",
    "abc7ny.com",
    "abcactionnews.com",
    "abcn.ws",
    "aljazeera.com",
    "antena3.com",
    "atresplayer.com",
    "bfmtv.com",
    "boston25news.com",
    "cadenaser.com",
    "canal-plus.com",
    "canal.fr",
    "cbs.com",
    "cbsloc.al",
    "cbslocal.com",
    "cbsn.ws",
    "cbsnews.com",
    "channel7breakingreport.live",
    "cnbc.com",
    "cnn.com",
    "cnn.it",
    "cope.es",
    "cuatro.com",
    "europe1.fr",
    "firstcoastnews.com",
    "fox.com",
    "fox13news.com",
    "fox13now.com",
    "fox2now.com",
    "fox4kc.com",
    "fox59.com",
    "fox6now.com",
    "fox8.com",
    "fox9.com",
    "foxbusiness.com",
    "foxla.com",
    "foxnews.com",
    "globo.com",
    "goodmorningamerica.com",
    "insideedition.com",
    "itv.com",
    "komonews.com",
    "lasexta.com",
    "lbc.co.uk",
    "local10.com",
    "local12.com",
    "mediaset.it",
    "msnbc.com",
    "nbc.com",
    "nbcchicago.com",
    "nbcdfw.com",
    "nbclosangeles.com",
    "nbcnews.com",
    "nbcnews.to",
    "nbcnewyork.com",
    "ndtv.com",
    "news12.com",
    "news4jax.com",
    "news5cleveland.com",
    "ondacero.es",
    "primocanale.it",
    "prosieben.at",
    "rac1.cat",
    "radioclassique.fr",
    "radioitalia.it",
    "rtl.fr",
    "sky.com",
    "sky.it",
    "skytg24news.it",
    "telecinco.es",
    "telemundo.com",
    "tf1.fr",
    "tgcom24.it",
    "weau.com",
    "wpxi.com",
    "wsbtv.com",
    "wxyz.com",
    "24economia.com",
    "affaritaliani.it",
    "agoravox.fr",
    "arcamax.com",
    "bento.de",
    "blitzquotidiano.it",
    "breaknotizie.com",
    "businessinsider.com",
    "businessinsider.de",
    "buzzfeed.com",
    "buzzfeednews.com",
    "caffeinamagazine.it",
    "cnews.fr",
    "ctxt.es",
    "economiadigital.es",
    "elboletin.com",
    "elconfidencial.com",
    "elconfidencialdigital.com",
    "eldiario.es",
    "eldigitalcastillalamancha.es",
    "elespanol.com",
    "estrelladigital.es",
    "fanpage.it",
    "finanzen.net",
    "fivethirtyeight.com",
    "francesoir.fr",
    "gasteizhoy.com",
    "huffingtonpost.co.uk",
    "huffingtonpost.com",
    "huffingtonpost.es",
    "huffingtonpost.fr",
    "huffingtonpost.it",
    "huffpost.com",
    "infolibre.es",
    "kentonline.co.uk",
    "lainformacion.com",
    "lapresse.it",
    "lavozdelsur.es",
    "leggioggi.it",
    "lettoquotidiano.it",
    "libertaddigital.com",
    "linternaute.com",
    "linternaute.fr",
    "livenewsnow.com",
    "livesicilia.it",
    "mediapart.fr",
    "naciodigital.cat",
    "news-mondo.it",
    "news-und-nachrichten.de",
    "news.com.au",
    "news.de",
    "news64.net",
    "newsbreakapp.com",
    "newser.com",
    "newsmondo.it",
    "newsnow.co.uk",
    "nextquotidiano.it",
    "noticias24.com",
    "notizie.it",
    "politico.com",
    "presseportal.de",
    "publico.es",
    "quifinanza.it",
    "quotidianodiragusa.it",
    "racocatala.cat",
    "realclearpolitics.com",
    "republica.com",
    "rosenheim24.de",
    "roughlyexplained.com",
    "salon.com",
    "slate.com",
    "slate.fr",
    "strettoweb.com",
    "tempi.it",
    "termometropolitico.it",
    "theconversation.com",
    "thedailybeast.com",
    "theperspective.com",
    "timesofisrael.com",
    "uol.com.br",
    "valenciaplaza.com",
    "vice.com",
    "vilaweb.cat",
    "vox.com",
    "vozpopuli.com",
    "wired.it",
    "worldjusticenews.com",
    "abc.net.au",
    "ard-text.de",
    "ardmediathek.de",
    "bbc.co.uk",
    "bbc.com",
    "cbc.ca",
    "ccma.cat",
    "channel4.com",
    "france.tv",
    "france24.com",
    "france3.fr",
    "francebleu.fr",
    "franceculture.fr",
    "franceinter.fr",
    "francetelevisions.fr",
    "francetv.fr",
    "francetvinfo.fr",
    "heute.de",
    "npr.org",
    "orf.at",
    "pbs.org",
    "rai.it",
    "rainews.it",
    "raiplay.it",
    "raiplayradio.it",
    "rtbf.be",
    "rte.ie",
    "rtve.es",
    "sbs.com.au",
    "swr.de",
    "tagesschau.de",
    "tv5monde.com",
    "uktv.co.uk",
    "voanews.com",
    "wdr2.de",
    "abc.es",
    "actu.fr",
    "actualites-la-croix.com",
    "adnkronos.com",
    "ansa.it",
    "apnews.com",
    "ara.cat",
    "avvenire.it",
    "baltimoresun.com",
    "bergamopost.it",
    "bloomberg.com",
    "bostonglobe.com",
    "canarias7.es",
    "capital.fr",
    "cataniatoday.it",
    "challenges.fr",
    "chicagotribune.com",
    "corriere.it",
    "corriereadriatico.it",
    "courrierdelouest.fr",
    "courrierinternational.com",
    "dallasnews.com",
    "daytondailynews.com",
    "democratandchronicle.com",
    "denverpost.com",
    "derbytelegraph.co.uk",
    "diaridegirona.cat",
    "diaridetarragona.com",
    "diariocordoba.com",
    "diariodeibiza.es",
    "diariodeleon.es",
    "diariodemallorca.es",
    "diariodenavarra.es",
    "diariodesevilla.es",
    "diarioinformacion.com",
    "diariojaen.es",
    "diariolibre.com",
    "diariosur.es",
    "diariovasco.com",
    "ecodibergamo.it",
    "economist.com",
    "el-nacional.com",
    "elcomercio.es",
    "elcorreo.com",
    "elcorreogallego.es",
    "elcorreoweb.es",
    "eldia.es",
    "eldiariomontanes.es",
    "eleconomista.es",
    "elmundo.es",
    "elnortedecastilla.es",
    "elpais.com",
    "elperiodico.com",
    "elperiodicoextremadura.com",
    "elpuntavui.cat",
    "eltiempo.com",
    "estrepublicain.fr",
    "euro-actu.fr",
    "europapress.es",
    "expansion.com",
    "expressandstar.com",
    "forbes.com",
    "fortune.com",
    "ft.com",
    "gazzettadelsud.it",
    "gazzettadiparma.it",
    "giornaledibrescia.it",
    "giornaledilecco.it",
    "giornaledimonza.it",
    "giornaletrentino.it",
    "giornalone.it",
    "granadahoy.com",
    "heraldo.es",
    "hoy.es",
    "huelvainformacion.es",
    "humanite.fr",
    "ideal.es",
    "ilcorrieredellacitta.com",
    "ilfattoquotidiano.it",
    "ilfoglio.it",
    "ilgazzettino.it",
    "ilgiornale.it",
    "ilgiorno.it",
    "ilmanifesto.it",
    "ilmattino.it",
    "ilmessaggero.it",
    "ilsecoloxix.it",
    "ilsole24ore.com",
    "iltempo.it",
    "independent.co.uk",
    "indiatimes.com",
    "inews.co.uk",
    "internazionale.it",
    "jpost.com",
    "kieler-nachrichten.de",
    "la-croix.com",
    "lagacetadesalamanca.es",
    "lagazzettadelmezzogiorno.it",
    "lamarea.com",
    "lanouvellerepublique.fr",
    "lanuevacronica.com",
    "laopinioncoruna.es",
    "laopiniondemalaga.es",
    "laopiniondemurcia.es",
    "laopiniondezamora.es",
    "lapresse.ca",
    "laprovence.com",
    "laprovincia.es",
    "larazon.es",
    "larepublica.pe",
    "larioja.com",
    "lasprovincias.es",
    "lastampa.it",
    "latimes.com",
    "latribune.fr",
    "lavanguardia.com",
    "laverdad.es",
    "lavoixdunord.fr",
    "lavozdegalicia.es",
    "lavozdigital.es",
    "ledauphine.com",
    "lefigaro.fr",
    "lemonde.fr",
    "lep.co.uk",
    "lepoint.fr",
    "lepopulaire.fr",
    "lesechos.fr",
    "lexpress.fr",
    "liberation.fr",
    "liberoquotidiano.it",
    "lincolnshirelive.co.uk",
    "lne.es",
    "malagahoy.es",
    "marianne.net",
    "mercurynews.com",
    "miamiherald.com",
    "milanotoday.it",
    "motherjones.com",
    "naiz.eus",
    "nationalreview.com",
    "newsday.com",
    "newsok.com",
    "newsweek.com",
    "nouvelobs.com",
    "nymag.com",
    "nytimes.com",
    "observer-reporter.com",
    "oggitreviso.it",
    "oregonlive.com",
    "orlandosentinel.com",
    "ouest-france.fr",
    "panorama.it",
    "paris-normandie.fr",
    "plymouthherald.co.uk",
    "post-gazette.com",
    "quotidiano.net",
    "quotidianodipuglia.it",
    "quotidianopiemontese.it",
    "regio7.cat",
    "repubblica.it",
    "republicain-lorrain.fr",
    "reuters.com",
    "riminitoday.it",
    "romatoday.it",
    "seattletimes.com",
    "sfgate.com",
    "startribune.com",
    "sudinfo.be",
    "sudouest.fr",
    "sun-sentinel.com",
    "telegraph.co.uk",
    "theatlantic.com",
    "theguardian.com",
    "thehill.com",
    "thetelegraphandargus.co.uk",
    "thetimes.co.uk",
    "time.com",
    "torinotoday.it",
    "trevisotoday.it",
    "udinetoday.it",
    "usnews.com",
    "valeursactuelles.com",
    "washingtonexaminer.com",
    "washingtonpost.com",
    "washingtontimes.com",
    "waz.de",
    "wired.com",
    "wsj.com",
    "20minutes.fr",
    "20minutos.es",
    "20mn.fr",
    "blick.de",
    "dailymail.co.uk",
    "dailyrecord.co.uk",
    "dailystar.co.uk",
    "eveningtimes.co.uk",
    "express.co.uk",
    "hulldailymail.co.uk",
    "ilrestodelcarlino.it",
    "journaldemontreal.com",
    "lanazione.it",
    "leggo.it",
    "leparisien.fr",
    "metro.co.uk",
    "metro.it",
    "mirror.co.uk",
    "nydailynews.com",
    "nypost.com",
    "parismatch.com",
    "que.es",
    "standard.co.uk",
    "suntimes.com",
    "thesun.co.uk",
    "trinitymirror-news.co.uk",
    "usatoday.com",
    "infosperber.ch",
]

# News organization page name patterns
news_org_patterns = [
    r"^DR\s",  # DR followed by space
    r"^DR$",  # DR exact match
]


def extract_urls_from_text(text: str) -> List[str]:
    """Extract URLs from text content using regex."""
    if not isinstance(text, str):
        return []

    # Pattern matches http/https URLs
    url_pattern = r"https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+"
    return re.findall(url_pattern, text)


def extract_domain(url: str) -> str:
    """Extract domain from URL and remove www."""
    try:
        parsed = urlparse(url)
        domain = re.sub(r"^www\.", "", parsed.netloc.lower())
        return domain
    except:
        return ""


def classify_news_source(domain: str) -> str:
    """Classify a domain as alternative, mainstream, or other."""
    if domain is None:
        return "invalid_url"

    # Check alternative news sources - use exact domain matching
    if domain in alternative_news_sources:
        return "alternative"

    # Check mainstream news sources - use exact domain matching
    if domain in mainstream_news_sources:
        return "mainstream"

    return "other"


def parse_facebook_timestamp(timestamp: Any) -> Optional[datetime]:
    """Parse Facebook timestamp to datetime object.
    Handles various timestamp formats and validates the result."""
    if not timestamp:
        return None

    try:
        # If already a datetime
        if isinstance(timestamp, datetime):
            # Validate the date is not near epoch
            if timestamp.year < 2000:
                return None
            return timestamp

        # If timestamp is a string
        if isinstance(timestamp, str):
            # Remove timezone offset as we'll standardize to UTC
            clean_timestamp = timestamp.split("+")[0]
            try:
                if "T" in clean_timestamp:
                    dt = datetime.strptime(clean_timestamp, "%Y-%m-%dT%H:%M:%S")
                else:
                    dt = datetime.strptime(clean_timestamp, "%Y-%m-%d %H:%M:%S")
                # Validate the date is not near epoch
                if dt.year < 2000:
                    return None
                return dt
            except ValueError:
                return None

        # If timestamp is a number (Unix timestamp)
        if isinstance(timestamp, (int, float)):
            # Filter out obviously invalid timestamps (too small)
            if (
                timestamp < 946684800
            ):  # Before year 2000 (Unix timestamp for 2000-01-01)
                return None

            try:
                dt = datetime.fromtimestamp(timestamp)
                # Validate the date is reasonable (not too far in the future)
                if dt.year >= 2000 and dt.year <= 2030:
                    return dt
                return None
            except (ValueError, OSError):
                # Try milliseconds if seconds didn't work
                try:
                    dt = datetime.fromtimestamp(timestamp / 1000)
                    if dt.year >= 2000 and dt.year <= 2030:
                        return dt
                    return None
                except (ValueError, OSError):
                    return None

    except Exception as e:
        print(f"Error parsing timestamp {timestamp}: {e}")
        return None


def extract_content_from_item(item: Dict) -> str:
    """Extract content from a Facebook data item, handling various structures."""
    content = ""

    # For posts and comments with 'data' list
    if "data" in item and isinstance(item["data"], list):
        for data_item in item["data"]:
            if isinstance(data_item, dict):
                # Try different possible content fields
                content = (
                    data_item.get("post", "")
                    or data_item.get("comment", "")
                    or data_item.get("text", "")
                    or ""
                )
                if content:
                    break

    # For items with direct 'title' field
    if not content and "title" in item:
        content = item["title"]

    # For items with attachments
    if "attachments" in item and isinstance(item["attachments"], list):
        for attachment in item["attachments"]:
            if isinstance(attachment, dict):
                if isinstance(attachment.get("data"), dict):
                    content = attachment["data"].get("text", "") or content
                elif isinstance(attachment.get("data"), list):
                    for data_item in attachment["data"]:
                        if isinstance(data_item, dict):
                            content = data_item.get("text", "") or content
                            if content:
                                break

    # For items with media
    if "media" in item and isinstance(item["media"], list):
        for media_item in item["media"]:
            if isinstance(media_item, dict):
                content = (
                    media_item.get("description", "")
                    or media_item.get("title", "")
                    or content
                )
                if content:
                    break

    return content


def process_json_file(file_path: str) -> List[Dict[str, Any]]:
    """Process a JSON file and extract URLs with metadata."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        results = []
        items_to_process = []

        # Handle different file structures
        if isinstance(data, dict):
            # Handle v2 structures
            if "comments_v2" in data:
                items_to_process = data["comments_v2"]
            elif "group_posts_v2" in data:
                items_to_process = data["group_posts_v2"]
            elif "group_comments_v2" in data:
                items_to_process = data["group_comments_v2"]
            elif "posts" in data:
                items_to_process = data["posts"]
        elif isinstance(data, list):
            items_to_process = data

        if not isinstance(items_to_process, list):
            items_to_process = [items_to_process]

        for item in items_to_process:
            if not isinstance(item, dict):
                continue

            # Extract timestamp
            timestamp = item.get("timestamp", "")

            # Extract content using the helper function
            content = extract_content_from_item(item)

            if content:
                urls = extract_urls_from_text(content)
                if urls:
                    for url in urls:
                        domain = extract_domain(url)
                        classification = classify_news_source(domain)
                        results.append(
                            {
                                "timestamp": parse_facebook_timestamp(timestamp),
                                "url": url,
                                "domain": domain,
                                "classification": classification,
                                "source_file": os.path.basename(file_path),
                            }
                        )

        return results
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        import traceback

        print(f"Full error: {traceback.format_exc()}")
        return []


def process_followed_pages(file_path: str) -> List[Dict[str, Any]]:
    """Process the pages_you've_liked.json file to find followed news pages."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        results = []
        pages_list = []

        # Handle v2 structure
        if isinstance(data, dict) and "page_likes_v2" in data:
            pages_list = data["page_likes_v2"]

        if not isinstance(pages_list, list):
            print(f"Unexpected data structure in {file_path}")
            if isinstance(pages_list, dict):
                print(f"Available keys: {list(pages_list.keys())}")
            return results

        for page in pages_list:
            if not isinstance(page, dict):
                continue

            # Extract page name, timestamp and url
            page_name = page.get("name", "")
            timestamp = page.get("timestamp", "")
            url = page.get("url", "")

            # Check if the page name matches any news organization pattern
            for pattern in news_org_patterns:
                # If pattern starts with ^ or ends with $, treat as regex
                if pattern.startswith("^") or pattern.endswith("$"):
                    if re.search(pattern, page_name, re.IGNORECASE):
                        results.append(
                            {
                                "timestamp": datetime.fromtimestamp(timestamp)
                                if timestamp
                                else None,
                                "page_name": page_name,
                                "matched_pattern": pattern,
                                "url": url,
                                "source_file": os.path.basename(file_path),
                                "type": "liked_page",
                            }
                        )
                        break
                # Otherwise do exact substring match (case insensitive)
                elif pattern.lower() in page_name.lower():
                    results.append(
                        {
                            "timestamp": datetime.fromtimestamp(timestamp)
                            if timestamp
                            else None,
                            "page_name": page_name,
                            "matched_pattern": pattern,
                            "url": url,
                            "source_file": os.path.basename(file_path),
                            "type": "liked_page",
                        }
                    )
                    break

        return results
    except Exception as e:
        print(f"Error processing liked pages {file_path}: {e}")
        import traceback

        print(f"Full error: {traceback.format_exc()}")
        return []


def extract_actor_from_title(title: str) -> str:
    """Extract the actor/account name from a reaction title."""
    try:
        # Pattern matches anything between "synes godt om " and either "s " or end of string
        # This captures both possessive forms ("Johns billede") and regular forms ("John Green")
        match = re.search(r"synes godt om ([^']+?)(?:s\s|$)", title)
        if match:
            return match.group(1).strip()
        return ""
    except:
        return ""


def process_reactions_file(file_path: str) -> List[Dict[str, Any]]:
    """Process likes and reactions file to find interactions with news sources."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        results = []

        if not isinstance(data, list):
            print(f"Unexpected data structure in {file_path}")
            return results

        for item in data:
            if not isinstance(item, dict):
                continue

            # Extract timestamp and title
            timestamp = item.get("timestamp", "")
            title = item.get("title", "")

            if not title:
                continue

            # Extract the actor/account name from the title
            actor = extract_actor_from_title(title)

            if actor:
                # Check if the actor matches any news organization pattern
                for pattern in news_org_patterns:
                    # If pattern starts with ^ or ends with $, treat as regex
                    if pattern.startswith("^") or pattern.endswith("$"):
                        if re.search(pattern, actor, re.IGNORECASE):
                            results.append(
                                {
                                    "timestamp": datetime.fromtimestamp(timestamp)
                                    if timestamp
                                    else None,
                                    "page_name": actor,
                                    "matched_pattern": pattern,
                                    "interaction_type": "reaction",
                                    "source_file": os.path.basename(file_path),
                                    "original_title": title,
                                }
                            )
                            break
                    # Otherwise do exact substring match (case insensitive)
                    elif pattern.lower() in actor.lower():
                        results.append(
                            {
                                "timestamp": datetime.fromtimestamp(timestamp)
                                if timestamp
                                else None,
                                "page_name": actor,
                                "matched_pattern": pattern,
                                "interaction_type": "reaction",
                                "source_file": os.path.basename(file_path),
                                "original_title": title,
                            }
                        )
                        break

        return results
    except Exception as e:
        print(f"Error processing reactions file {file_path}: {e}")
        import traceback

        print(f"Full error: {traceback.format_exc()}")
        return []


def analyze_facebook_directory(base_dir: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Analyze Facebook data directory for news sources and followed pages."""
    # Define paths to relevant JSON files
    json_paths = [
        "your_facebook_activity/comments_and_reactions/comments.json",
        "your_facebook_activity/groups/group_posts_and_comments.json",
        "your_facebook_activity/groups/your_comments_in_groups.json",
        "your_facebook_activity/posts/posts_on_other_pages_and_profiles.json",
    ]

    pages_path = "your_facebook_activity/pages/pages_you've_liked.json"
    reactions_path = "your_facebook_activity/comments_and_reactions"
    recently_viewed_path = "logged_information/interactions/recently_viewed.json"

    all_url_results = []
    all_page_results = []
    all_reactions_results = []

    # Process URLs from posts and comments
    for json_path in json_paths:
        full_path = os.path.join(base_dir, json_path)
        if os.path.exists(full_path):
            results = process_json_file(full_path)
            all_url_results.extend(results)

    # Process recently viewed content
    recently_viewed_full_path = os.path.join(base_dir, recently_viewed_path)
    if os.path.exists(recently_viewed_full_path):
        recently_viewed_results = process_recently_viewed(recently_viewed_full_path)
        all_url_results.extend(recently_viewed_results)
        print(f"Found {len(recently_viewed_results)} recently viewed news items")

    # Process liked pages
    pages_full_path = os.path.join(base_dir, pages_path)
    if os.path.exists(pages_full_path):
        page_results = process_followed_pages(pages_full_path)
        all_page_results.extend(page_results)

    # Process reactions
    reactions_full_path = os.path.join(base_dir, reactions_path)
    if os.path.exists(reactions_full_path):
        # Find all likes_and_reactions files
        for i in range(1, 100):  # Reasonable upper limit
            reactions_file = os.path.join(
                reactions_full_path, f"likes_and_reactions_{i}.json"
            )
            if os.path.exists(reactions_file):
                results = process_reactions_file(reactions_file)
                all_reactions_results.extend(results)
            else:
                break  # Stop when we don't find the next file

    # Create DataFrames
    df_urls = pd.DataFrame(all_url_results) if all_url_results else pd.DataFrame()
    df_pages = (
        pd.DataFrame(all_page_results + all_reactions_results)
        if (all_page_results or all_reactions_results)
        else pd.DataFrame()
    )

    # Sort by timestamp if data exists
    if not df_urls.empty:
        df_urls = df_urls.sort_values("timestamp")
    if not df_pages.empty:
        df_pages = df_pages.sort_values("timestamp")

    return df_urls, df_pages


def save_analysis(
    df_urls: pd.DataFrame,
    df_pages: pd.DataFrame,
    output_dir: str = "../data/processed/",
):
    """Save analysis results to Excel files."""
    if df_urls.empty and df_pages.empty:
        print("No data to save.")
        return

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Save URLs analysis
    if not df_urls.empty:
        urls_file = f"{output_dir}facebook_shared_urls_{timestamp}.xlsx"
        df_urls.to_excel(urls_file, index=False)
        print(f"URLs analysis saved to: {urls_file}")

    # Save followed pages analysis
    if not df_pages.empty:
        pages_file = f"{output_dir}facebook_followed_pages_{timestamp}.xlsx"
        df_pages.to_excel(pages_file, index=False)
        print(f"Followed pages analysis saved to: {pages_file}")


def process_recently_viewed(file_path: str) -> List[Dict[str, Any]]:
    """Process recently_viewed.json file to extract news source URLs."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        results = []

        # Navigate through the nested structure
        if "recently_viewed" in data:
            for section in data["recently_viewed"]:
                if "children" in section:
                    for child in section["children"]:
                        if "entries" in child:
                            for entry in child["entries"]:
                                timestamp = entry.get("timestamp")
                                entry_data = entry.get("data", {})

                                # Extract URI/URL
                                uri = entry_data.get("uri", "")
                                name = entry_data.get("name", "")
                                watch_time = entry_data.get("watch_time", "0")

                                if uri:
                                    # Extract domain from URI using Facebook-aware extraction
                                    domain = extract_news_domain_from_facebook_url(
                                        uri, name
                                    )
                                    if domain:
                                        # Classify the domain
                                        classification = classify_news_source(domain)

                                        # Only include news sources (mainstream or alternative)
                                        if classification in [
                                            "mainstream",
                                            "alternative",
                                        ]:
                                            results.append(
                                                {
                                                    "timestamp": parse_facebook_timestamp(
                                                        timestamp
                                                    ),
                                                    "url": uri,
                                                    "domain": domain,
                                                    "classification": classification,
                                                    "name": name,
                                                    "watch_time_seconds": int(
                                                        watch_time
                                                    )
                                                    if watch_time.isdigit()
                                                    else 0,
                                                    "source_file": os.path.basename(
                                                        file_path
                                                    ),
                                                    "content_type": "recently_viewed",
                                                }
                                            )

        return results

    except Exception as e:
        print(f"Error processing recently viewed file {file_path}: {e}")
        return []


def extract_news_domain_from_facebook_url(url: str, name: str = "") -> str:
    """Extract the actual news domain from Facebook URLs.

    For Facebook URLs like https://facebook.com/DailyMaildotcom, we need to
    map them to the actual news domain like dailymail.com.
    """
    try:
        parsed = urlparse(url)
        if "facebook.com" in parsed.netloc:
            # Extract the page name from the URL path
            path_parts = parsed.path.strip("/").split("/")
            if path_parts:
                page_name = path_parts[0].lower()

                # Check if the page name corresponds to a known news source
                # First try direct mapping for common patterns
                domain_mappings = {
                    "dailymaildotcom": "dailymail.com",
                    "dailymailuk": "dailymail.com",
                    "cnn": "cnn.com",
                    "bbc": "bbc.com",
                    "bbcnews": "bbc.com",
                    "theguardian": "theguardian.com",
                    "guardian": "theguardian.com",
                    "dr.dk": "dr.dk",
                    "politiken.dk": "politiken.dk",
                    "berlingske.dk": "berlingske.dk",
                    "tv2": "tv2.dk",
                    "ekstrabladet": "ekstrabladet.dk",
                    "bt": "bt.dk",
                    "information": "information.dk",
                    "jyllandsposten": "jyllands-posten.dk",
                    "jyllands-posten": "jyllands-posten.dk",
                    "documentdk": "document.dk",
                    "dokument": "document.dk",
                    "denkorteavis": "denkorteavis.dk",
                    "redox": "redox.dk",
                    "arbejderen": "arbejderen.dk",
                }

                # Check direct mappings first
                if page_name in domain_mappings:
                    return domain_mappings[page_name]

                # Check if page name contains a known news domain
                for alt_domain in alternative_news_sources:
                    if alt_domain.replace(".", "").replace("-", "") in page_name:
                        return alt_domain

                for main_domain in mainstream_news_sources:
                    if main_domain.replace(".", "").replace("-", "") in page_name:
                        return main_domain

                # If we have a name from the data, try to match it as well
                if name:
                    name_lower = name.lower()
                    for alt_domain in alternative_news_sources:
                        if (
                            alt_domain.replace(".dk", "").replace(".com", "")
                            in name_lower
                        ):
                            return alt_domain
                    for main_domain in mainstream_news_sources:
                        if (
                            main_domain.replace(".dk", "").replace(".com", "")
                            in name_lower
                        ):
                            return main_domain

        # Fall back to regular domain extraction
        return extract_domain(url)

    except Exception:
        return extract_domain(url)


def extract_domain(url: str) -> str:
    """Extract domain from URL and remove www."""
    try:
        parsed = urlparse(url)
        domain = re.sub(r"^www\.", "", parsed.netloc.lower())
        return domain
    except:
        return ""


def main():
    # Base directory for Facebook data - using absolute path
    base_dir = "/Users/Codebase/projects/alteruse/data/Kantar_download_398_unzipped_new/474-4477-c-146161_2025-05-01T20__4477g1746131161115sKJC67TKXu0ju5259uu5259ufacebookJulietjalve01052025KYBSSakb-UFTx1n3"

    print("Analyzing Facebook data for URLs and followed pages...")
    df_urls, df_pages = analyze_facebook_directory(base_dir)

    # Print URL analysis results
    if not df_urls.empty:
        print("\nSummary of URLs Found:")
        print("-" * 40)

        # Show all domains and their counts
        print("\nAll Domains (Top 10):")
        print(df_urls["domain"].value_counts().head(10))

        # Show classification distribution
        print("\nClassification Distribution:")
        print(df_urls["classification"].value_counts())

        # Show counts by source file
        print("\nURLs found by source file:")
        print(df_urls["source_file"].value_counts())

        # For news sources, show top domains
        print("\nTop Domains by Classification:")
        for classification in ["mainstream", "alternative", "other"]:
            print(f"\nTop {classification} sources:")
            print(
                df_urls[df_urls["classification"] == classification]["domain"]
                .value_counts()
                .head(5)
            )
    else:
        print("\nNo URLs found in the data.")

    # Print followed pages analysis results
    if not df_pages.empty:
        print("\nSummary of Followed News Pages:")
        print("-" * 40)

        # Show all followed news pages
        print("\nAll followed news pages:")
        for _, row in df_pages.iterrows():
            print(f"- {row['page_name']} (matched: {row['matched_pattern']})")

        # Show counts by pattern
        print("\nNews organizations followed:")
        print(df_pages["matched_pattern"].value_counts())
    else:
        print("\nNo news pages found in followed pages.")

    # Save results
    save_analysis(df_urls, df_pages)


if __name__ == "__main__":
    main()
