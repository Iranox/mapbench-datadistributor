use benchmark;

ALTER TABLE `vendor`
DROP PRIMARY KEY ,
ADD PRIMARY KEY (`nr`);

ALTER TABLE `person`
DROP PRIMARY KEY ,
ADD PRIMARY KEY (`nr`),
ADD CONSTRAINT fk_publisherperson FOREIGN KEY (publisher) REFERENCES vendor(nr);

ALTER TABLE `producer`
DROP PRIMARY KEY ,
ADD PRIMARY KEY (`nr`,`publisher`),
ADD CONSTRAINT fk_publisherproducer FOREIGN KEY (publisher) REFERENCES person(nr);
 
ALTER TABLE `product`
DROP PRIMARY KEY ,
ADD PRIMARY KEY (`nr`),
ADD CONSTRAINT fk_producerproduct FOREIGN KEY (producer) REFERENCES person(nr),
ADD CONSTRAINT fk_publisherproduct FOREIGN KEY (publisher) REFERENCES person(nr);

 ALTER TABLE `productfeature`
DROP PRIMARY KEY ,
 ADD PRIMARY KEY (`nr`),
 ADD CONSTRAINT fk_publisherproductfeature FOREIGN KEY (publisher) REFERENCES person(nr);

 
ALTER TABLE `productfeatureproduct`
DROP PRIMARY KEY ,
ADD PRIMARY KEY (`product`,`productFeature`),
ADD CONSTRAINT fk_productfeatureproduct FOREIGN KEY (product) REFERENCES product(nr),
ADD CONSTRAINT fk_productfeaturefeature FOREIGN KEY (productFeature) REFERENCES productfeature(nr);
  
ALTER TABLE `producttype`
DROP PRIMARY KEY ,
ADD PRIMARY KEY (`nr`),
ADD CONSTRAINT fk_publisherproducttype FOREIGN KEY (publisher) REFERENCES person(nr);

ALTER TABLE `producttypeproduct`
DROP PRIMARY KEY ,
ADD PRIMARY KEY (`product`,`productType`),
ADD CONSTRAINT fk_producttypeproduct FOREIGN KEY (product) REFERENCES product(nr),
ADD CONSTRAINT fk_producttypefeature FOREIGN KEY (productType) REFERENCES producttype(nr);

ALTER TABLE `review`
DROP PRIMARY KEY ,
ADD PRIMARY KEY (`nr`),  
ADD CONSTRAINT fk_productreview FOREIGN KEY (product) REFERENCES product(nr),
ADD CONSTRAINT fk_producerreview FOREIGN KEY (producer) REFERENCES producer(nr),
ADD CONSTRAINT fk_personreview FOREIGN KEY (person) REFERENCES person(nr),
ADD CONSTRAINT fk_publisherreview FOREIGN KEY (publisher) REFERENCES person(nr);

ALTER TABLE `offer`
DROP PRIMARY KEY ,
ADD PRIMARY KEY (`nr`),
ADD CONSTRAINT fk_productoffer FOREIGN KEY (product) REFERENCES product(nr),
ADD CONSTRAINT fk_produceroffer FOREIGN KEY (producer) REFERENCES producer(nr),
ADD CONSTRAINT fk_vendoroffer FOREIGN KEY (vendor) REFERENCES vendor(nr),
ADD CONSTRAINT fk_publisheroffer FOREIGN KEY (publisher) REFERENCES person(nr);



