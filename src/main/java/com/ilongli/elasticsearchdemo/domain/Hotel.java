package com.ilongli.elasticsearchdemo.domain;

import lombok.Data;

/**
 * @author ilongli
 * @date 2022/8/23 15:08
 */
@Data
public class Hotel {

    private Long id;
    private String name;
    private String address;
    private Integer price;
    private Integer score;
    private String brand;
    private String city;
    private String starName;
    private String business;
    private String longitude;
    private String latitude;
    private String pic;

}
