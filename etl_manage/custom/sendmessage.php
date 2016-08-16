<?php

class Message {

	private $url = "http://115.29.204.76:82/message.json";
    private $key = "2c4e0";
    private $secret = "ea1cb1804484e1226b8fd07830aff633";
    
    
    public function send($phone='',$content='') {
        if (empty($phone) || empty($content)) return false;

    	$date = date('c');
    	$token = md5($this->secret.$date);
    	
    	$headers['Content-Type'] = 'application/json-rpc';
        $headers['Authorization'] = "PLAYCRAB ".$this->key.":".$token;
        $headers['Date'] = $date;
        
        $send_data = array(
            "jsonrpc" => "2.0",
            "method" => "sendMessage",
            "params" => array("channel" => 1,"phone" => $phone,"content" => $content)
        );
        
        $this->send_request($headers,$send_data);
        
    }
    
    
    /**
     * 发送请求 ，获取返回值
     * @param array $headers
     * @param array $data
     */
    private function send_request(array $headers,array $data) {
        $headerArr = array();
        foreach($headers as $key=>$val) {
            $headerArr[] = $key.':'.$val;
        }
        
        //请求
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL,$this->url);
        curl_setopt($ch, CURLOPT_POST,1);
        curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($data));
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_HTTPHEADER, $headerArr);
        $tt = curl_exec($ch);
        curl_close($ch);
        
        echo $tt;
    }


}


#接收命令行参数：电话号码、报警内容
$phone = $_SERVER["argv"][1];
$content = $_SERVER["argv"][2];

date_default_timezone_set('Asia/Shanghai');
$i = new Message();
$i -> send($phone, $content);
