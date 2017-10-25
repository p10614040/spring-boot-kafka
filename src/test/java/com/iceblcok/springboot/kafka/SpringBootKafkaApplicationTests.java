package com.iceblcok.springboot.kafka;

import com.iceblcok.springboot.kafka.producer.KafkaSenderService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Date;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringBootKafkaApplicationTests {

    @Test
    public void contextLoads() {

    }

    @Autowired
    private KafkaSenderService senderService;

    /**
     * 测试发送消息到 Kafka，消息会被 KafkaReceiverListener 接收
     *
     * @throws IOException
     */
    @Test
    public void sendAlarm() throws IOException, InterruptedException {
        // 发送消息数量
        int num = 50000;
        // topic 与 KafkaReceiverListener 中 topics 相对应
        String topic = "kafkaspeedtest";
        for (int i = 0; i < num; i++) {
            senderService.sendMessage(topic, "<AlarmStart>\n" +
                    "\tIntVersion:V1.0\n" +
                    "\tMsgSerial:" + System.currentTimeMillis() +"\n" +
                    "\tAlarmUniqueId:450925624_2046889893_783558721_1352265129\n" +
                    "\tClearId:1436747353_1717605091_2453870295_1076667139\n" +
                    "\tStandardFlag:2\n" +
                    "\tSubAlarmType:0\n" +
                    "\tNeId:PTP-04e06c903ab46ab7013ab637cbd0091e\n" +
                    "\tLocateNeName:网块33_JR-大岭大禾洞-202-惠东MF(FHD19R4环)-1-1-2-E1_16-01\n" +
                    "\tLocateNeType:Port\n" +
                    "\tNeName:网块33_JR-大岭大禾洞-202-惠东MF(FHD19R4环)\n" +
                    "\tNeAlias:special_field22=#_#\n" +
                    "\tEquipmentClass:SDH\n" +
                    "\tNeIp:\n" +
                    "\tSystemName:\n" +
                    "\tVendor:202\n" +
                    "\tVersion:v1.0\n" +
                    "\tLocateNeStatus:1\n" +
                    "\tProjectNo:\n" +
                    "\tProjectName:\n" +
                    "\tProjectStartTime:\n" +
                    "\tProjectEndTime:\n" +
                    "\tLocateInfo:\n" +
                    "\tEventTime:2017-09-12 09:11:04\n" +
                    "\tCancelTime:\n" +
                    "\tDalTime:\n" +
                    "\tVendorAlarmType:设备告警\n" +
                    "\tVendorSeverity:3\n" +
                    "\tAlarmSeverity:3\n" +
                    "\tVendorAlarmId:0\n" +
                    "\tNmsAlarmId:024-060-00-800443\n" +
                    "\tAlarmStatus:1\n" +
                    "\tAckFlag:0\n" +
                    "\tAckTime:\n" +
                    "\tAckUser:\n" +
                    "\tAlarmTitle:PPI_AIS\n" +
                    "\tStandardAlarmName:PPI_AIS\n" +
                    "\tProbableCauseTxt:AIS\n" +
                    "\tAlarmText:&lt;AlarmStart&gt; IntVersion:V3.0 AlarmUniqueId:CURRENT_ALARM-ff8080815e26e7b2015e73a3cdcd6d7c NeName:网块33_JR-大岭大禾洞-202-惠东MF(FHD19R4环) SystemName:传输网管 EquipmentClass:SDH Vendor:烽火 Version:软件版本:4.1.17 LocateNeName:网块33_JR-大岭大禾洞-202-惠东MF(FHD19R4环)-1-1-2-E1_16-01 LocateNeType:物理端口 LocateNeStatus:正常 LocateInfo://网块33_JR-大岭大禾洞-202-惠东MF(FHD19R4环)-1-1-2-E1_16-01 EventTime:2017-09-12 09:11:04 CancelTime: VendorAlarmType: 设备告警 AlarmType:设备告警 VendorSeverity:3 AlarmSeverity:3 VendorAlarmId:100665060_1_4_20170912091104.0_20170912091104.0 NmsAlarmId:024-060-00-800443 AlarmStatus:1 AlarmTitle:PPI_AIS ProbableCauseTxt:AIS AlarmText:alarmlocation=IN:CircuitInfo=:AlarmObjectMEName=网块33_JR-大岭大禾洞-202-惠东MF(FHD19R4环):affectedSNCName=:rtuRecieveTime=20170912091108.484 Specialty:5 AlarmLogicClass:通信 AlarmLogicSubClass:通道 EffectOnEquipment: EffectOnBusiness:业务受影响 NmsAlarmType:1 AlarmProvince:广东 AlarmRegion:惠州 NetManagerServerity:次要告警 AlarmEms:OTNM-HZSDH-1 service_level:6 county_name:null CableSectionNo: CodeMc: MAINTAINDEPARTMENT: BtsId:PTP-04e06c903ab46ab7013ab637cbd0091e &lt;AlarmEnd&gt;\n" +
                    "\tCircuitNo:PTP-04e06c903ab46ab7013ab637cbd0091e\n" +
                    "\tPortRate:\n" +
                    "\tSpecialty:2\n" +
                    "\tBusinessSystem:\n" +
                    "\tAlarmLogicClass:35\n" +
                    "\tAlarmLogicSubClass:132\n" +
                    "\tEffectOnEquipment:0\n" +
                    "\tEffectOnBusiness:3\n" +
                    "\tNmsAlarmType:1\n" +
                    "\tSendGroupFlag:0\n" +
                    "\tRelatedFlag:\n" +
                    "\tAlarmProvince:广东\n" +
                    "\tAlarmRegion:惠州\n" +
                    "\tAlarmCounty:惠东县\n" +
                    "\tSite:\n" +
                    "\tAlarmActCount:\n" +
                    "\tCorrelateAlarmFlag:\n" +
                    "\tSheetSendStatus:\n" +
                    "\tSheetStatus:\n" +
                    "\tSheetNo:\n" +
                    "\tAlarmMemo:NeType=SDH#_#alarmLevel=3#_#acceptMan=#_#acceptManId=#_#copyMan=戴幸平#_#copyManId=daixp5#_#speciality=4#_#mobileNetType=4002#_#TnmsFaultType=0#_#SiteType=4#_#rate=#_#EQP_Type=6#_#RoomID=ROOM-20C7E0E02042438688B1452B1161512E#_#RoomName=惠州惠东大岭大禾洞一楼机房一#_#sheetType=#_#copySpeciality=#_#faultType=#_#roomLevel=0#_#roomProperty=0#_#\n" +
                    "<AlarmEnd>#@#@1505178668187>");
        }

        // 阻塞，使消息可以被监听接收
        System.in.read();
    }

    @Test
    public void sendEsbFile() throws IOException, InterruptedException {
        // 发送消息数量
        int num = 1;
        // topic 与 KafkaReceiverListener 中 topics 相对应
        String topic = "FILE_INFO_JSON";
        for (int i = 0; i < num; i++) {
            senderService.sendMessage(topic, "{\"collBatchId\":\"59dad370498ee5a21d5796be\",\"collFileId\":0,\"collSubtaskId\":\"192.168.42.191/nmsdata/SD/WCDMA/MOBILE/EVERSEC/CNOS/CXDR/20171009/SD_JN_MOBILE_CNOS_EVERSEC_CXDR_objectType_0001_20171009093905_Gn_231095_0.txt.gz\",\"collTaskCode\":\"FTPCollect_GT3gTRACEib1doku97\",\"dataSources\":\"291\",\"dataSourcesType\":0,\"ddlType\":0,\"fileCreatTime\":1507513211367,\"fileDataType\":2,\"fileLength\":\"5497404\",\"fileName\":\"SD_JN_MOBILE_CNOS_EVERSEC_CXDR_objectType_0001_20171009093905_Gn_231095_0.txt.gz\",\"filePath\":\"192.168.42.191/nmsdata/SD/WCDMA/MOBILE/EVERSEC/CNOS/CXDR/20171009/SD_JN_MOBILE_CNOS_EVERSEC_CXDR_objectType_0001_20171009093905_Gn_231095_0.txt.gz\",\"fileStatus\":3,\"fileStopTime\":1507513226706,\"fileType\":0,\"fileUploadTime\":1507513200000,\"logUpdatetime\":1507513226707,\"province\":\"SD\",\"saveDataDuration\":\"15339\"}");
        }

        // 阻塞，使消息可以被监听接收
        System.in.read();
    }

}
