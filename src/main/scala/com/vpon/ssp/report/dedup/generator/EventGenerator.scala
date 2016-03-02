// scalastyle:off
package com.vpon.ssp.report.dedup.generator

import org.scalacheck.{Arbitrary, Gen}

import com.vpon.ssp.report.dedup.generator.crypto.DES
import com.vpon.trade.Device.DeviceTime
import com.vpon.trade.Event.EVENTTYPE
import com.vpon.trade.Media.{ADPOSITIONTYPE, MEDIATYPE, SUPPLYTYPE}
import com.vpon.trade.Site.PROTOCOLTYPE
import com.vpon.trade.User.GENDERTYPE
import com.vpon.trade._

object EventGenerator {

  val secretKeyMap = Map[String, String]("vpon" -> "#1$6%^8&", "appnexus" -> "%23!8$@#", "doubleclick" -> "#*2@3&4%")

  val dspIdList = Seq("6", "10", "15", "18", "23")

  val dspGroupIdList = Seq("vpon", "appnexus", "doubleclick")

  val genTradeLog: Gen[TradeLog] = for {
    bidId <- genIdString
    bidTimestamp <- genTimestamp
    impressionType <- genImpressionTypeForTradeLog
    placement <- genPlacement
    media <- genMedia
    device <- genDevice
    user <- Gen.option(genUser)
    site <- genSite
    app <- genApp
    (site, app) <- Gen.oneOf((Some(site), None), (None, Some(app)))
    winDspId <- Gen.oneOf(dspIdList)
    winDspGroupId <- Gen.oneOf(dspGroupIdList)
    winBidResult <- genWinBidResult(placement.`bidFloor`, winDspId, winDspGroupId)
    losers <- genLostBidResultList(placement.`bidFloor`, winDspId)
    sortedPriceList <- genSortedPriceList(winBidResult, losers)
    clearPrice <- genClearPrice(placement.`bidFloor`, sortedPriceList)
    rate <- genRate
    winner <- Gen.option(genBidWinner(winDspId, winDspGroupId, placement.`bidFloor`, clearPrice))
    sdk <- Gen.option(genSdk)
  } yield {
      new TradeLog(bidId,
        bidTimestamp,
        impressionType,
        placement,
        media,
        device,
        rate,
        user,
        site,
        app,
        winner,
        scala.collection.immutable.Seq(losers: _*))
    }

  def genEventByTradeLog(tradeLog: TradeLog): Gen[Event] = {
    tradeLog.`winner` match {
      case Some(s: BidWinner) => {
        for {
          tmpImpression <- genImpression(
            `bidTimestamp` = tradeLog.`bidTimestamp`,
            `bidId` = tradeLog.`bidId`,
            `placement` = tradeLog.`placement`,
            `media` = tradeLog.`media`,
            `device` = tradeLog.`device`,
            `rate` = tradeLog.`rate`,
            `user` = tradeLog.`user`,
            `site` = tradeLog.`site`,
            `app` = tradeLog.`app`,
            `winner` = tradeLog.`winner`.get)

          tmpClick <- genClick(
            `bidTimestamp` = tradeLog.`bidTimestamp`,
            `bidId` = tradeLog.`bidId`,
            `placement` = tradeLog.`placement`,
            `media` = tradeLog.`media`,
            `device` = tradeLog.`device`,
            `rate` = tradeLog.`rate`,
            `user` = tradeLog.`user`,
            `site` = tradeLog.`site`,
            `app` = tradeLog.`app`,
            `winner` = tradeLog.`winner`.get)

          (tradeLog, impression, click) <- Gen.oneOf((Some(tradeLog), None, None), (None, Some(tmpImpression), None), (None, None, Some(tmpClick)))
        } yield {
          val (eventType, eventKey) = getEventTypeAndEventKey(tradeLog, impression, click)
          new Event(eventType, eventKey, tradeLog, impression, click)
        }
      }
      case None => {
        val event_key = s"t_${tradeLog.`bidId`}"
        Gen.const(new Event(EVENTTYPE.TRADELOG, event_key, Some(tradeLog), None, None))
      }
    }
  }

  val genEvent: Gen[Event] = for {
    tmpTradeLog <- genTradeLog
    event <- genEventByTradeLog(tmpTradeLog)
  } yield event

  implicit val eventGen: Arbitrary[Event] = Arbitrary {
    genEvent
  }

  val genHasWinnerTradeLog: Gen[TradeLog] = for {
    bidId <- genIdString
    bidTimestamp <- genTimestamp
    impressionType <- genImpressionTypeForTradeLog
    placement <- genPlacement
    media <- genMedia
    device <- genDevice
    user <- Gen.option(genUser)
    site <- genSite
    app <- genApp
    (site, app) <- Gen.oneOf((Some(site), None), (None, Some(app)))
    winDspId <- Gen.oneOf(dspIdList)
    winDspGroupId <- Gen.oneOf(dspGroupIdList)
    winBidResult <- genWinBidResult(placement.`bidFloor`, winDspId, winDspGroupId)
    losers <- genLostBidResultList(placement.`bidFloor`, winDspId)
    sortedPriceList <- genSortedPriceList(winBidResult, losers)
    clearPrice <- genClearPrice(placement.`bidFloor`, sortedPriceList)
    rate <- genRate
    winner <- genBidWinner(winDspId, winDspGroupId, placement.`bidFloor`, clearPrice)
    sdk <- Gen.option(genSdk)
  } yield {
      new TradeLog(bidId,
        bidTimestamp,
        impressionType,
        placement,
        media,
        device,
        rate,
        user,
        site,
        app,
        Some(winner),
        scala.collection.immutable.Seq(losers: _*))
    }

  def genGroupedEventsByTradeLog(tradeLog: TradeLog): Gen[Seq[Event]] = {
    for {
      tmpImpression <- genImpression(
        `bidTimestamp` = tradeLog.`bidTimestamp`,
        `bidId` = tradeLog.`bidId`,
        `placement` = tradeLog.`placement`,
        `media` = tradeLog.`media`,
        `device` = tradeLog.`device`,
        `rate` = tradeLog.`rate`,
        `user` = tradeLog.`user`,
        `site` = tradeLog.`site`,
        `app` = tradeLog.`app`,
        `winner` = tradeLog.`winner`.get)

      tmpClick <- genClick(
        `bidTimestamp` = tradeLog.`bidTimestamp`,
        `bidId` = tradeLog.`bidId`,
        `placement` = tradeLog.`placement`,
        `media` = tradeLog.`media`,
        `device` = tradeLog.`device`,
        `rate` = tradeLog.`rate`,
        `user` = tradeLog.`user`,
        `site` = tradeLog.`site`,
        `app` = tradeLog.`app`,
        `winner` = tradeLog.`winner`.get)
    } yield {
      Seq(
        new Event(EVENTTYPE.TRADELOG, s"t_${tradeLog.`bidId`}", Some(tradeLog), None, None),
        new Event(EVENTTYPE.IMPRESSION, s"i_${tmpImpression.`impressionId`}", None, Some(tmpImpression.copy(`impressionTimestamp` = tradeLog.`bidTimestamp`)), None),
        new Event(EVENTTYPE.CLICK, s"c_${tmpClick.`clickId`}", None, None, Some(tmpClick.copy(`clickTimestamp` = tradeLog.`bidTimestamp`)))
      )
    }
  }

  val genGroupedEvents: Gen[Seq[Event]] = for {
    tmpTradeLog <- genHasWinnerTradeLog
    events <- genGroupedEventsByTradeLog(tmpTradeLog)
  } yield events

  implicit val groupedEventsGen: Arbitrary[Seq[Event]] = Arbitrary {
    genGroupedEvents
  }

  def genTimestampEarlierThan(givenTimestamp: Long): Gen[Long] = Gen.choose(givenTimestamp - (10000 * Math.random()).toLong, givenTimestamp)

  def genPlatformId: Gen[String] = Gen.const("1")

  def genTimestamp: Gen[Long] = {
    val currentTimestamp = System.currentTimeMillis
    //    Gen.choose(currentTimestamp - 1000000, currentTimestamp + 1000000)
    Gen.const(currentTimestamp)
  }

  def genImpressionTypeForImpressionOrClick: Gen[IMPRESSIONTYPE.EnumVal] =
    Gen.oneOf(IMPRESSIONTYPE.DEFAULT, IMPRESSIONTYPE.KEPT, IMPRESSIONTYPE.PSA, IMPRESSIONTYPE.RESOLD, IMPRESSIONTYPE.RTB, IMPRESSIONTYPE.EXTERNAL_CLICK, IMPRESSIONTYPE.EXTERNAL_IMPRESSION)

  def genImpressionTypeForTradeLog: Gen[IMPRESSIONTYPE.EnumVal] =
    Gen.oneOf(IMPRESSIONTYPE.BLANK, IMPRESSIONTYPE.DEFAULT, IMPRESSIONTYPE.KEPT, IMPRESSIONTYPE.PSA, IMPRESSIONTYPE.RESOLD, IMPRESSIONTYPE.RTB, IMPRESSIONTYPE.EXTERNAL_CLICK, IMPRESSIONTYPE.EXTERNAL_IMPRESSION)

  // must provide bidFloor, bidFloorCur although they are defined as optional
  def genPlacement: Gen[Placement] = for {
    placementId <- genPlacementId
    platformId <- genPlatformId
    bidFloor <- Gen.choose(100.00, 200.00)
    bidFloorCur <- genCurrency
  } yield Placement(placementId, platformId, bidFloor, bidFloorCur)

  def genDouble(min: Double, max: Double): Gen[Double] = {
    Gen.choose(min, max)
  }

  def genCurrency: Gen[String] = Gen.oneOf("CNY", "TWD", "HKD", "JPY", "USD")

  def genClearPrice(bidFloor: Double, sortedPriceList: Seq[Double]): Gen[Double] = {
    val clearPrice = {
      if (sortedPriceList.isEmpty)
        0
      else if (sortedPriceList.length == 1)
        bidFloor
      else
        sortedPriceList(1)
    }
    Gen.const(clearPrice)
  }

  def genWinBidResult(bidfloor: Double, winDspId: String, winDspGroupId: String): Gen[WinBid] = for {
    dspId <- Gen.const(winDspId)
    dspGroupId <- Gen.const(winDspGroupId)
    bidPrice <- genWinBidPrice(bidfloor)
  } yield WinBid(dspId, Some(dspGroupId), bidPrice)

  def genLostBidResultList(bidfloor: Double, winDspId: String): Gen[List[BidResult]] = {
    val allLostDspIds = dspIdList diff Seq(winDspId)
    import scala.collection.JavaConverters._
    Gen.someOf(allLostDspIds).flatMap(lostDspIds =>
      Gen.sequence(lostDspIds.map(t => genLostBidResult(bidfloor, t))).flatMap(arrayList => arrayList.asScala.toList))
  }

  def genLostBidResult(bidfloor: Double, dspId: String): Gen[BidResult] = for {
    dspId <- Gen.const(dspId)
    bidLostReason <- Gen.oneOf(
      BIDLOSTREASON.UNKNOWN_BIDLOSTREASON,
      BIDLOSTREASON.LOW_PRICE,
      BIDLOSTREASON.TIMEOUT,
      BIDLOSTREASON.ADM_INVALID,
      BIDLOSTREASON.RESPONSE_INVALID,
      BIDLOSTREASON.BID_ADM_TITLE_INVALID,
      BIDLOSTREASON.BID_AUCTION_TYPE_INVALID,
      BIDLOSTREASON.BID_ID_INVALID,
      BIDLOSTREASON.BID_PRICE_INVALID,
      BIDLOSTREASON.BID_SEAT_INVALID,
      BIDLOSTREASON.BID_WIN_NOTICE_INVALID,
      BIDLOSTREASON.NOT_BIDDING)
    bidPrice <- Gen.option(genBidPrice(bidfloor))
    currency <- Gen.option(genCurrency)
    dealType <- Gen.option(genDealType)
    dspGroupId <- Gen.option(Gen.oneOf(dspGroupIdList))
  } yield {
      val bp:Option[Double] = bidPrice match {
        case Some(b) => Some(b)
        case None => None
      }
      BidResult(dspId, bidLostReason, bp, currency, dealType, dspGroupId)
    }

  def genBidPrice(bidfloor: Double): Gen[Double] = genDouble(bidfloor.toDouble, bidfloor.toDouble + 50)

  def genSortedPriceList(winBid: WinBid, lostBidResultList: Seq[BidResult]): Gen[Seq[Double]] = {
    var priceList:Seq[Double] = for (lostBidResult <- lostBidResultList if (BIDLOSTREASON.LOW_PRICE.equals(lostBidResult.`bidLostReason`))) yield {
      lostBidResult.`bidPrice` match {
        case Some(b) => b
        case None => 0
      }
    }
    priceList = priceList.+:(winBid.bidPrice)
    val sortedPriceList = priceList.sortWith(_ > _)
    Gen.const(sortedPriceList)
  }

  def genBidWinner(winDspId: String, winDspGroupId: String, bidfloor: Double, clearPrice: Double): Gen[BidWinner] = for {
    dspId <- Gen.const(winDspId)
    dspGroupId <- Gen.const(winDspGroupId)
    bidPrice <- genWinBidPrice(bidfloor)
    clearPrice <- Gen.const(clearPrice)
    dealType <- genDealType
    currency <- genCurrency
  } yield new BidWinner(winDspId, bidfloor, clearPrice, dealType, currency, Some(dspGroupId))

  def genDealType: Gen[DEALTYPE.EnumVal] = Gen.oneOf(
    //    DEALTYPE.CPA,
    DEALTYPE.CPM,
    DEALTYPE.CPC)

  def genWinBidPrice(bidfloor: Double): Gen[Double] = genDouble(bidfloor + 60, bidfloor + 70)

  def genMediaFormat: Gen[String] = Gen.oneOf("html", "flash", "jpg", "gif", "png")

  def genIFrameState: Gen[Option[Media.IFRAMESTATETYPE.EnumVal]] =
    Gen.option(Gen.oneOf(Media.IFRAMESTATETYPE.SAME_DOMAIN_IFRAME, Media.IFRAMESTATETYPE.CROSS_DOMAIN_IFRAME, Media.IFRAMESTATETYPE.NO_IFRAME, Media.IFRAMESTATETYPE.UNKNOWN_IFRAMESTATETYPE))

  def genMedia: Gen[Media] = for {
    mediaType <- genMediaType
    (mediaWidth, mediaHeight) <- genSize
    supplyType <- genSupplyType
    adPosition <- Gen.option(genAdPosition)
    mediaFormat <- genMediaFormat
    iframeState <- genIFrameState
  } yield new Media(mediaType, mediaWidth, mediaHeight, supplyType, adPosition, mediaFormat, iframeState)

  def genMediaType: Gen[MEDIATYPE.EnumVal] = Gen.oneOf(MEDIATYPE.BANNER, MEDIATYPE.INTERSTITIAL)

  def genSupplyType: Gen[SUPPLYTYPE.EnumVal] =
    Gen.oneOf(SUPPLYTYPE.APPS_PLACEMENTS, SUPPLYTYPE.MOBILE_WEB_PLACEMENTS, SUPPLYTYPE.WEB_PLACEMENTS, SUPPLYTYPE.TV_PLACEMENTS, SUPPLYTYPE.UNKNOWN_SUPPLYTYPE)

  def genAdPosition: Gen[ADPOSITIONTYPE.EnumVal] =
    Gen.oneOf(ADPOSITIONTYPE.ABOVE_FOLD, ADPOSITIONTYPE.BELOW_FOLD, ADPOSITIONTYPE.DEPRECATED, ADPOSITIONTYPE.FOOTER, ADPOSITIONTYPE.FULLSCREEN, ADPOSITIONTYPE.HEADER, ADPOSITIONTYPE.SIDEBAR)

  def genSize: Gen[(Int, Int)] = Gen.oneOf((320, 50), (360, 50), (640,100), (720,100))

  def genLength: Gen[Option[Int]] = Gen.option(Gen.choose(50, 500))

  def genLengthLessThan(given: Option[Int]): Gen[Option[Int]] = {
    given match {
      case Some(l) => Gen.option(Gen.choose(10, l))
      case None => Gen.option(Gen.const(10))
    }
  }

  def genSdkVersion: Gen[String] = Gen.oneOf("1.0", "1.1", "1.0.5", "1.2")

  def genSdk: Gen[SDK] = for {
    version <- genSdkVersion
  } yield new SDK(version)

  def genScreenDensity: Gen[Double] = Gen.choose(0.1, 3.9)
  def genCxnt: Gen[String] = for { k <- Gen.choose(0, 9) } yield k.toString
  def genTelt: Gen[String] = for { k <- Gen.choose(0, 9) } yield k.toString
  def genMcc: Gen[String] = for { k <- Gen.choose(400, 500) } yield k.toString
  def genMnc: Gen[String] = for { k <- Gen.choose(0, 99) } yield { if (k < 10) s"0$k" else k.toString }

  def genDeviceTime: Gen[DeviceTime] = for {
    clientTimestamp <- Gen.option(genTimestamp)
    clientTimezone <- Gen.option(Gen.choose(-11, 12))
  } yield new DeviceTime(clientTimestamp, clientTimezone)

  def genDevice: Gen[Device] = for {
    userAgent <- genUserAgent
    clientIp <- genClientIP
    language <- genLanguage
    screenWidth <- genLength
    screenHeight <- genLength
    screenAvailableWidth <- genLengthLessThan(screenWidth)
    screenAvailableHeight <- genLengthLessThan(screenHeight)
    screenDensity <- Gen.option(genScreenDensity)
    cxnt <- Gen.option(genCxnt)
    telt <- Gen.option(genTelt)
    mcc <- Gen.option(genMcc)
    mnc <- Gen.option(genMnc)
    deviceTime <- Gen.option(genDeviceTime)
  } yield new Device(userAgent, clientIp, language, screenAvailableHeight, screenAvailableWidth, screenHeight, screenWidth, screenDensity, cxnt, telt, mcc, mnc, deviceTime)

  def genUserAgent: Gen[String] = Gen.const(RandomUserAgent.getRandomUserAgent)
  //  def genUserAgent: Gen[String] = Gen.const("Mozilla/5.0 (iPad; U; CPU OS 3_2_1 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Mobile/7B405")

  def genClientIP = for {
    a <- Gen.choose(0, 255)
    b <- Gen.choose(0, 255)
    c <- Gen.choose(0, 255)
    d <- Gen.choose(0, 255)
  } yield s"$a.$b.$c.$d"
  //  def genClientIP = Gen.const("0.0.0.0")

  def genLanguage: Gen[String] = Gen.oneOf("zh-CN", "en-US", "zh-TW", "ja")

  def genUser: Gen[User] = for {
    age <- genAge
    gender <- genGender
  } yield new User(age, gender)

  def genAge: Gen[Option[Int]] = Gen.option(Gen.choose(15, 65))

  def genGender: Gen[Option[GENDERTYPE.EnumVal]] = Gen.option(Gen.oneOf(GENDERTYPE.MALE, GENDERTYPE.FEMALE, GENDERTYPE.UNKNOWN_GENDERTYPE))

  def genSite: Gen[Site] = for {
    siteDomain <- genSiteDomain
    siteProtocol <- genSiteProtocol
  } yield new Site(siteDomain, siteProtocol)

  def genSiteDomain: Gen[String] = Gen.oneOf("appnexus", "netflix", "google", "yahoo", "amazon")

  def genSiteProtocol: Gen[PROTOCOLTYPE.EnumVal] = Gen.oneOf(PROTOCOLTYPE.HTTP, PROTOCOLTYPE.HTTPS)

  def genApp: Gen[App] = for {
    appId <- genIdString
  } yield new App(appId)

  def genRate: Gen[Rate] = for {
    exchangeRateVersion <- Gen.choose(1, 2)
    publisherSspTaxRateVersion <- Gen.choose(1, 2)
    dspSspTaxRateVersion <- Gen.choose(1, 2)
  } yield new Rate(exchangeRateVersion, publisherSspTaxRateVersion, dspSspTaxRateVersion)

  def genImpression(bidTimestamp: Long, bidId: String,
                    placement: Placement, media: Media, device: Device, rate: Rate, user: Option[User],
                    site: Option[Site], app: Option[App], winner: BidWinner): Gen[Impression] = {

    for {
      impressionType <- genImpressionTypeForImpressionOrClick
      //      impressionId <- genIdString
      impressionTimestamp <- genTimestampLaterThan(bidTimestamp)
    } yield {
      new Impression(bidId, // impressionId == bidId
        impressionType,
        impressionTimestamp,
        bidTimestamp,
        bidId,
        encryptPrice(winner.`clearPrice`, winner.`dspGroupId`),
        winner.`dspId`,
        winner.`dealType`,
        placement,
        media,
        device,
        rate,
        user,
        site,
        app,
        winner.`dspGroupId`)
    }
  }

  def genIdString: Gen[String] = {
    //    Gen.identifier
    for {
      k <- Gen.uuid
    } yield {
      k.toString
    }
  }

  def genPlacementId: Gen[String] = Gen.choose(1, 2).map(num => num.toString)

  def genTimestampLaterThan(givenTimestamp: Long): Gen[Long] = Gen.const(givenTimestamp) //Gen.choose(givenTimestamp, givenTimestamp + 1000000)

  def encryptPrice(price: Double, optDspGroupId: Option[String]): String = {
    optDspGroupId match {
      case None => ""
      case Some(dspGroupId) => {
        secretKeyMap.get(dspGroupId) match {
          case Some(secretKey) => DES.base64DesEncrypt(price.toString(), secretKey.asInstanceOf[String])
          case None => ""
        }
      }
    }
  }

  def genClick(bidTimestamp: Long, bidId: String, placement: Placement,
               media: Media, device: Device, rate: Rate, user: Option[User], site: Option[Site],
               app: Option[App], winner: BidWinner): Gen[Click] = {
    for {
      impressionType <- genImpressionTypeForImpressionOrClick
      //      clickId <- genIdString
      clickTimestamp <- genTimestampLaterThan(bidTimestamp)
    } yield {
      new Click(bidId, // clickId == bidId
        impressionType,
        clickTimestamp,
        bidTimestamp,
        bidId,
        encryptPrice(winner.`clearPrice`, winner.`dspGroupId`),
        winner.`dspId`,
        winner.`dealType`,
        placement,
        media,
        device,
        rate,
        user,
        site,
        app,
        winner.`dspGroupId`)
    }
  }

  def getEventTypeAndEventKey(tradeLog: Option[TradeLog], impression: Option[Impression], click: Option[Click]): (EVENTTYPE.EnumVal, String) =
    (tradeLog, impression, click) match {
      case (Some(x), None, None) => (EVENTTYPE.TRADELOG, s"t_${x.`bidId`}")
      case (None, Some(x), None) => (EVENTTYPE.IMPRESSION, s"i_${x.`impressionId`}")
      case (None, None, Some(x)) => (EVENTTYPE.CLICK, s"c_${x.`clickId`}")
      case _ => throw new RuntimeException("Unknown Event Type!")
    }

  case class WinBid(dspId: String, dspGroupId: Option[String], bidPrice: Double)
}
// scalastyle:on
